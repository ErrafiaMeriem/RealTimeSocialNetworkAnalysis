from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    lower,
    trim,
    regexp_replace,
    sha2,
    udf
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException


# =========================
# CONFIG
# =========================
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "reddit_comments"

MONGO_URI = "mongodb://mongodb:27017"
MONGO_DB = "reddit_db"
MONGO_COLLECTION = "comments_clean"


# =========================
# SCHEMA
# =========================
comment_schema = StructType([
    StructField("comment_id", StringType(), True),
    StructField("post_id", StringType(), True),
    StructField("body", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("created_at", StringType(), True),
    StructField("author", StringType(), True),
])


# =========================
# SPARK SESSION
# =========================
spark = (
    SparkSession.builder
    .appName("RedditCommentsKafkaBatch")
    .config("spark.mongodb.write.connection.uri", MONGO_URI)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Starting Kafka batch processing")
print("=" * 60)


# =========================
# READ FROM KAFKA (BATCH)
# =========================
raw_df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

if raw_df.count() == 0:
    print("No messages in Kafka topic. Exiting safely.")
    spark.stop()
    exit(0)


# =========================
# PARSE JSON
# =========================
df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), comment_schema).alias("data"))
    .select("data.*")
    .filter(col("comment_id").isNotNull())
)

if df.count() == 0:
    print("No valid JSON records after parsing. Exiting.")
    spark.stop()
    exit(0)


# =========================
# ADVANCED TEXT CLEANING
# =========================
df = df.withColumnRenamed("body", "comment_body")

df = (
    df
    .filter(col("comment_body").isNotNull())
    .withColumn("comment_body", lower(col("comment_body")))
)

# 1️⃣ Normalize URLs → <URL>
df = df.withColumn(
    "comment_body",
    regexp_replace(
        col("comment_body"),
        r"(https?://\S+|www\.\S+)",
        "<url>"
    )
)

# 2️⃣ Remove escaped newlines, tabs, slashes from scraping
df = df.withColumn(
    "comment_body",
    regexp_replace(
        col("comment_body"),
        r"(\\n|\\r|\\t|\\|\/)",
        " "
    )
)

# 3️⃣ **NEW: Remove ALL escaped quotes (\", \') - converts to regular quotes first**
df = df.withColumn(
    "comment_body",
    regexp_replace(col("comment_body"), r'\\"', '"')
)
df = df.withColumn(
    "comment_body",
    regexp_replace(col("comment_body"), r"\\'", "'")
)

# 4️⃣ **NEW: Remove ALL double quotes (since they're typically formatting, not content)**
df = df.withColumn(
    "comment_body",
    regexp_replace(col("comment_body"), r'"', '')
)

# 5️⃣ **NEW: Clean up multiple apostrophes or backticks**
df = df.withColumn(
    "comment_body",
    regexp_replace(col("comment_body"), r"('{2,}|`{2,})", "")
)

# 5️⃣ **NEW: Clean up multiple apostrophes or backticks**
df = df.withColumn(
    "comment_body",
    regexp_replace(col("comment_body"), r"('{2,}|`{2,})", "")
)

# 6️⃣ Remove markdown / Reddit formatting (*, **, __, ``` etc.)
df = df.withColumn(
    "comment_body",
    regexp_replace(col("comment_body"), r"[*_`]+", "")
)

# 7️⃣ Remove Reddit usernames (u/username)
df = df.withColumn(
    "comment_body",
    regexp_replace(col("comment_body"), r"u/\w+", "")
)

# 8️⃣ Remove hashtags but keep words (#palestine → palestine)
df = df.withColumn(
    "comment_body",
    regexp_replace(col("comment_body"), r"#(\w+)", r"\1")
)

# 9️⃣ Remove literal square brackets safely
df = df.withColumn(
    "comment_body",
    regexp_replace(col("comment_body"), r"[\[\]]", "")
)

# 9️⃣ **NEW: Clean up contractions with escaped quotes (couldn\'t → couldn t → couldn't)**
# First convert escaped apostrophes to normal ones
df = df.withColumn(
    "comment_body",
    regexp_replace(col("comment_body"), r"\\\'", "'")
)

# 🔟 Normalize whitespace (MUST be done AFTER quote removal)
df = df.withColumn(
    "comment_body",
    regexp_replace(col("comment_body"), r"\s+", " ")
)

df = df.withColumn("comment_body", trim(col("comment_body")))


# =========================
# FILTER BOT / WARNING / SPAM COMMENTS
# =========================
df = df.filter(
    ~col("comment_body").rlike(
        r"^(warning|help|n|bot|moderator|auto|this is an automated message)"
    )
)

# Donation / fundraising boilerplate
df = df.filter(
    ~col("comment_body").rlike(
        r"(donate|donation|fundraiser|give now|help palestinians in need|your donation delivers)"
    )
)

if df.count() == 0:
    print("All comments filtered out as noise/spam. Exiting.")
    spark.stop()
    exit(0)


# =========================
# CLEAN COMMENT (NO STOP WORD REMOVAL)
# =========================
df = df.withColumn("clean_comment", col("comment_body"))
df = df.filter(col("clean_comment") != "")


# =========================
# LANGUAGE FILTER (SAFE)
# =========================
def detect_lang_safe(text):
    try:
        return detect(text)
    except LangDetectException:
        return None

detect_lang_udf = udf(detect_lang_safe, StringType())

df = df.withColumn("language", detect_lang_udf(col("clean_comment")))
df = df.filter(col("language") == "en")

if df.count() == 0:
    print("No English comments after language filtering. Exiting.")
    spark.stop()
    exit(0)


# =========================
# DEDUPLICATION (CONTENT-BASED)
# =========================
df = df.withColumn(
    "comment_hash",
    sha2(col("clean_comment"), 256)
)

df = df.dropDuplicates(["comment_hash"])


# =========================
# FINAL SELECTION
# =========================
final_df = df.select(
    "comment_id",
    "post_id",
    "clean_comment",
    "comment_hash",
    "score",
    "created_at"
)

print(f"Final number of records to write: {final_df.count()}")


# =========================
# WRITE TO MONGODB
# =========================
(
    final_df.write
    .format("mongodb")
    .mode("append")
    .option("database", MONGO_DB)
    .option("collection", MONGO_COLLECTION)
    .save()
)

print("Write to MongoDB completed successfully.")
spark.stop()
print("Job completed successfully.")