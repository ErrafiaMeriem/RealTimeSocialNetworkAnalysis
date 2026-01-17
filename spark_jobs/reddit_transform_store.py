from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, lower, regexp_replace,
    trim, split, concat_ws
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.ml.feature import StopWordsRemover


# =========================
# CONFIG
# =========================
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "reddit_comments"

MONGO_URI = "mongodb://mongodb:27017"
MONGO_DB = "reddit_db"
MONGO_COLLECTION = "comments_clean"


# =========================
# SCHEMA (IMPORTANT)
# =========================
comment_schema = StructType([
    StructField("comment_id", StringType(), True),
    StructField("post_id", StringType(), True),
    StructField("body", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("created_at", StringType(), True),
])


# =========================
# SPARK SESSION
# =========================
spark = (
    SparkSession.builder
    .appName("RedditCommentsFromKafka")
    .config("spark.mongodb.write.connection.uri", MONGO_URI)
    .getOrCreate()
)


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

# value is binary → string → JSON
df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), comment_schema).alias("data"))
    .select("data.*")
)


# =========================
# TRANSFORMATIONS
# =========================

# rename body → comment_body
df = df.withColumnRenamed("body", "comment_body")

# lowercase
df = df.withColumn("comment_body", lower(col("comment_body")))

# remove u/username
df = df.withColumn(
    "comment_body",
    regexp_replace(col("comment_body"), r"u/\w+", "")
)

# remove warning comments
df = df.filter(~col("comment_body").startswith("warning"))

# trim
df = df.withColumn("comment_body", trim(col("comment_body")))

# remove duplicates
df = df.dropDuplicates(["comment_body"])

# tokenize
df = df.withColumn("tokens", split(col("comment_body"), "\\s+"))

# stopwords
remover = StopWordsRemover(
    inputCol="tokens",
    outputCol="clean_tokens"
)
df = remover.transform(df)

# rebuild text
df = df.withColumn(
    "clean_comment",
    concat_ws(" ", col("clean_tokens"))
)

# drop empty
df = df.filter(col("clean_comment") != "")


# =========================
# WRITE TO MONGODB
# =========================
(
    df.select(
        "comment_id",
        "post_id",
        "clean_comment",
        "score",
        "created_at"
    )
    .write
    .format("mongodb")
    .mode("append")
    .option("database", MONGO_DB)
    .option("collection", MONGO_COLLECTION)
    .save()
)

spark.stop()
