from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    lower,
    regexp_replace,
    trim,
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
print("Starting Kafka batch read")
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
# BASIC CLEANING (NO STOPWORDS)
# =========================
df = df.withColumnRenamed("body", "comment_body")

df = (
    df
    .filter(col("comment_body").isNotNull())
    .withColumn("comment_body", lower(col("comment_body")))
    .withColumn("comment_body", trim(col("comment_body")))
)

# Remove Reddit usernames safely
df = df.withColumn(
    "comment_body",
    regexp_replace(col("comment_body"), r"u/\w+", "")
)

# Remove literal square brackets safely
df = df.withColumn(
    "comment_body",
    regexp_replace(col("comment_body"), r"[\[\]]", "")
)

# Filter bot / deleted / warning comments
df = df.filter(
    ~col("comment_body").rlike(r"^(deleted|removed|warning)")
)

if df.count() == 0:
    print("All comments filtered out (bots / deleted / warnings). Exiting.")
    spark.stop()
    exit(0)


# =========================
# CLEAN COMMENT (KEEP STOPWORDS)
# =========================
df = df.withColumn(
    "clean_comment",
    col("comment_body")
)

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

df = df.withColumn(
    "language",
    detect_lang_udf(col("clean_comment"))
)

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
