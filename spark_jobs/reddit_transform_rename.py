from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, regexp_replace, trim,
    split, concat_ws
)
from pyspark.ml.feature import StopWordsRemover


# =========================
# CONFIG
# =========================
INPUT_PATH = "/opt/project/data/comments.json"

MONGO_URI = "mongodb://mongo:27017"
MONGO_DB = "reddit_db"
MONGO_COLLECTION = "comments_clean"


# =========================
# SPARK SESSION
# =========================
spark = (
    SparkSession.builder
    .appName("RedditCommentsCleaning")
    .config("spark.mongodb.write.connection.uri", MONGO_URI)
    .getOrCreate()
)


# =========================
# READ DATA
# =========================
df = spark.read.json(INPUT_PATH)


# =========================
# TRANSFORMATIONS
# =========================

# 1. lowercase
df = df.withColumn("comment_body", lower(col("comment_body")))

# 2. remove user mentions u/username
df = df.withColumn(
    "comment_body",
    regexp_replace(col("comment_body"), r"u/\w+", "")
)

# 3. remove comments starting with "warning"
df = df.filter(~col("comment_body").startswith("warning"))

# 4. remove extra spaces
df = df.withColumn("comment_body", trim(col("comment_body")))

# 5. remove duplicates
df = df.dropDuplicates(["comment_body"])

# 6. tokenize
df = df.withColumn(
    "tokens",
    split(col("comment_body"), "\\s+")
)

# 7. stop words removal
remover = StopWordsRemover(
    inputCol="tokens",
    outputCol="clean_tokens"
)

df = remover.transform(df)

# 8. rebuild clean text
df = df.withColumn(
    "clean_comment",
    concat_ws(" ", col("clean_tokens"))
)

# 9. drop empty comments
df = df.filter(col("clean_comment") != "")


# =========================
# WRITE TO MONGODB
# =========================
(
    df.select(
        "id",
        "post_id",
        "clean_comment",
        "score",
        "created_date"
    )
    .write
    .format("mongodb")
    .mode("append")
    .option("database", MONGO_DB)
    .option("collection", MONGO_COLLECTION)
    .save()
)

spark.stop()
