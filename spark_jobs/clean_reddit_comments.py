from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, regexp_replace, trim, length,
    split, size, when, lit
)
from pyspark.ml.feature import StopWordsRemover

# =========================
# CONFIG
# =========================
INPUT_PATH = "/opt/project/data/comments.json"

MONGO_URI = "mongodb://mongo:27017"
MONGO_DB = "reddit_db"
MONGO_COLLECTION = "comments_clean"

MIN_WORDS = 5

# =========================
# SPARK SESSION
# =========================
spark = (
    SparkSession.builder
    .appName("RedditCommentsCleaning")
    .config("spark.mongodb.write.connection.uri", MONGO_URI)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =========================
# LOAD COMMENTS
# =========================
df = spark.read.json(INPUT_PATH)

# =========================
# DROP NON-HUMAN COMMENTS
# =========================
df = df.filter(
    (col("author") != "AutoModerator") &
    (~col("body").isin("[deleted]", "[removed]")) &
    (~lower(col("body")).startswith("warning"))
)

# =========================
# REMOVE USER MENTIONS (u/username)
# =========================
df = df.withColumn(
    "body",
    regexp_replace(col("body"), r"\bu/[A-Za-z0-9_-]+\b", "")
)

# =========================
# HANDLE URLS
# =========================
df = df.withColumn(
    "has_url",
    col("body").rlike(r"http[s]?://")
)

df = df.withColumn(
    "body",
    regexp_replace(col("body"), r"http[s]?://\S+", "")
)

# =========================
# BASIC TEXT NORMALIZATION
# =========================
df = df.withColumn("body", lower(col("body")))
df = df.withColumn("body", regexp_replace(col("body"), r"[^a-z\s]", " "))
df = df.withColumn("body", regexp_replace(col("body"), r"\s+", " "))
df = df.withColumn("body", trim(col("body")))

# =========================
# FILTER SHORT COMMENTS
# =========================
df = df.withColumn("word_count", size(split(col("body"), " ")))
df = df.filter(col("word_count") >= MIN_WORDS)

# =========================
# DEDUPLICATION BY CONTENT
# =========================
df = df.dropDuplicates(["body"])

# =========================
# STOP WORDS REMOVAL
# =========================
df = df.withColumn("tokens", split(col("body"), " "))

remover = StopWordsRemover(
    inputCol="tokens",
    outputCol="tokens_clean"
)

df = remover.transform(df)

df = df.withColumn(
    "body_clean",
    trim(regexp_replace(
        col("tokens_clean").cast("string"),
        r"[\[\],]",
        ""
    ))
)

# =========================
# FINAL CLEANUP
# =========================
df_final = df.select(
    "comment_id",
    "post_id",
    "subreddit",
    "author",
    "body_clean",
    "has_url",
    "score",
    "created_utc",
    "created_at"
)

# =========================
# WRITE TO MONGODB
# =========================
(
    df_final.write
    .format("mongodb")
    .mode("append")
    .option("database", MONGO_DB)
    .option("collection", MONGO_COLLECTION)
    .save()
)

spark.stop()
