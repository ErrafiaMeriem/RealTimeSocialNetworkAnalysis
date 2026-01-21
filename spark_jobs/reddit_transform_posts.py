import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    lower,
    trim,
    regexp_replace,
    sha2,
    concat_ws,
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
KAFKA_TOPIC = "reddit_posts"

MONGO_URI = "mongodb://mongodb:27017"
MONGO_DB = "reddit_db"
MONGO_COLLECTION = "posts_clean"


# =========================
# SCHEMA (POSTS)
# =========================
post_schema = StructType([
    StructField("post_id", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("title", StringType(), True),
    StructField("selftext", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("created_at", StringType(), True),
    StructField("author", StringType(), True),
])


# =========================
# SPARK SESSION
# =========================
spark = (
    SparkSession.builder
    .appName("RedditPostsKafkaBatch")
    .config("spark.mongodb.write.connection.uri", MONGO_URI)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Starting Kafka batch processing for POSTS")
print("=" * 60)


# =========================
# READ FROM KAFKA
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
    .select(from_json(col("json_str"), post_schema).alias("data"))
    .select("data.*")
    .filter(col("post_id").isNotNull())
)

if df.count() == 0:
    print("No valid post records after parsing. Exiting.")
    spark.stop()
    exit(0)


# =========================
# COMBINE TEXT FIELDS
# =========================
df = df.withColumn(
    "post_text",
    concat_ws(" ", col("title"), col("selftext"))
)

df = (
    df
    .filter(col("post_text").isNotNull())
    .withColumn("post_text", lower(col("post_text")))
)


# =========================
# ADVANCED TEXT CLEANING (SAME LOGIC AS COMMENTS)
# =========================

# URLs → <url>
df = df.withColumn(
    "post_text",
    regexp_replace(col("post_text"), r"(https?://\S+|www\.\S+)", "<url>")
)

# Remove escaped junk
df = df.withColumn(
    "post_text",
    regexp_replace(col("post_text"), r"(\\n|\\r|\\t|\\\\|\\\/)", " ")
)

# Remove markdown / Reddit formatting
df = df.withColumn(
    "post_text",
    regexp_replace(col("post_text"), r"[*_`]+", "")
)

# Remove Reddit usernames
df = df.withColumn(
    "post_text",
    regexp_replace(col("post_text"), r"u/\w+", "")
)

# Remove hashtags but keep words
df = df.withColumn(
    "post_text",
    regexp_replace(col("post_text"), r"#(\w+)", r"\1")
)

# Remove literal square brackets
df = df.withColumn(
    "post_text",
    regexp_replace(col("post_text"), r"[\[\]]", "")
)

# Normalize whitespace
df = df.withColumn(
    "post_text",
    regexp_replace(col("post_text"), r"\s+", " ")
)

df = df.withColumn("post_text", trim(col("post_text")))


# =========================
# FILTER BOT / SPAM POSTS
# =========================
df = df.filter(
    ~col("post_text").rlike(
        r"^(warning|help|bot|moderator|auto|this is an automated message)"
    )
)

df = df.filter(
    ~col("post_text").rlike(
        r"(donate|donation|fundraiser|give now|your donation delivers)"
    )
)

if df.count() == 0:
    print("All posts filtered out as noise/spam. Exiting.")
    spark.stop()
    exit(0)


# =========================
# LANGUAGE FILTER
# =========================
def detect_lang_safe(text):
    try:
        return detect(text)
    except LangDetectException:
        return None

detect_lang_udf = udf(detect_lang_safe, StringType())

df = df.withColumn("language", detect_lang_udf(col("post_text")))
df = df.filter(col("language") == "en")

if df.count() == 0:
    print("No English posts after language filtering. Exiting.")
    spark.stop()
    exit(0)


# =========================
# DEDUPLICATION (POST CONTENT)
# =========================
df = df.withColumn(
    "post_hash",
    sha2(col("post_text"), 256)
)

df = df.dropDuplicates(["post_hash"])


# =========================
# FINAL SELECTION
# =========================
final_df = df.select(
    "post_id",
    "subreddit",
    "post_text",
    "post_hash",
    "score",
    "created_at"
)

print(f"Final number of posts to write: {final_df.count()}")


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
