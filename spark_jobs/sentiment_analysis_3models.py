from pyspark.sql import SparkSession
from pyspark.sql.functions import col, element_at, struct, lit, pandas_udf
from pyspark.sql.types import StringType, DoubleType
from pyspark.ml import PipelineModel

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob
import pandas as pd


# ============================================================
# Initialize analyzers
# ============================================================
vader_analyzer = SentimentIntensityAnalyzer()


# ============================================================
# Pandas UDFs
# ============================================================
@pandas_udf(StringType())
def vader_sentiment_udf(texts: pd.Series) -> pd.Series:
    def classify(text):
        if text is None or text.strip() == "":
            return "neutral"
        score = vader_analyzer.polarity_scores(text)["compound"]
        if score >= 0.05:
            return "positive"
        elif score <= -0.05:
            return "negative"
        else:
            return "neutral"

    return texts.apply(classify)


@pandas_udf(DoubleType())
def vader_confidence_udf(texts: pd.Series) -> pd.Series:
    return texts.fillna("").apply(
        lambda t: abs(vader_analyzer.polarity_scores(t)["compound"]) if t else 0.0
    )


@pandas_udf(DoubleType())
def textblob_polarity_udf(texts: pd.Series) -> pd.Series:
    return texts.fillna("").apply(
        lambda t: TextBlob(t).sentiment.polarity if t else 0.0
    )


@pandas_udf(DoubleType())
def textblob_subjectivity_udf(texts: pd.Series) -> pd.Series:
    return texts.fillna("").apply(
        lambda t: TextBlob(t).sentiment.subjectivity if t else 0.0
    )


def main():

    # ============================================================
    # Spark session
    # ============================================================
    spark = (
        SparkSession.builder
        .appName("RedditSentiment_3Models_CANONICAL_TEXT")
        .getOrCreate()
    )

    # ============================================================
    # Read SOURCE collection
    # ============================================================
    df = (
        spark.read
        .format("mongodb")
        .option("spark.mongodb.connection.uri", "mongodb://mongodb:27017")
        .option("spark.mongodb.database", "reddit_db")
        .option("spark.mongodb.collection", "comments_clean")
        .load()
    )

    # ============================================================
    # INCREMENTAL PROCESSING: Filter out already analyzed comments
    # ============================================================
    try:
        existing_analysed_df = (
            spark.read
            .format("mongodb")
            .option("spark.mongodb.connection.uri", "mongodb://mongodb:27017")
            .option("spark.mongodb.database", "reddit_db")
            .option("spark.mongodb.collection", "comments_analysed")
            .load()
            .select("comment_id")
            .distinct()
        )
    except Exception as e:
        print(f"Could not read comments_analysed collection (likely doesn't exist yet): {e}")
        existing_analysed_df = spark.createDataFrame([], schema="comment_id string")  # Empty DataFrame

    # Keep only new comments not yet analyzed
    df = df.join(existing_analysed_df, on="comment_id", how="left_anti")

    # If no new comments, exit early
    if df.count() == 0:
        print("No new comments to analyze. Exiting.")
        spark.stop()
        return

    # ============================================================
    # Select fields + canonical rename
    # ============================================================
    df = (
        df.select(
            "_id",
            "comment_id",
            "post_id",
            "clean_comment",
            "score",
            "created_at"
        )
        .withColumnRenamed("clean_comment", "text")  # 🔑 ONE canonical column
    )

    # ============================================================
    # Load Spark NLP pipeline
    # ============================================================
    pipeline = PipelineModel.load(
        "/opt/spark-nlp-models/analyze_sentiment_en_VIVEKN"
    )

    # ============================================================
    # Apply Spark NLP
    # ============================================================
    nlp_result = pipeline.transform(df)

    # ============================================================
    # Extract Vivekn sentiment
    # ============================================================
    nlp_result = (
        nlp_result
        .withColumn(
            "sentiment_vivekn",
            struct(
                element_at(col("sentiment.result"), 1).alias("label"),
                lit("analyze_sentiment_en_VIVEKN").alias("model"),
                lit("spark-nlp").alias("framework")
            )
        )
    )

    # ============================================================
    # Apply Pandas UDF models (ON text)
    # ============================================================
    final_df = (
        nlp_result
        .withColumn("vader_sentiment", vader_sentiment_udf(col("text")))
        .withColumn("vader_confidence", vader_confidence_udf(col("text")))
        .withColumn("textblob_polarity", textblob_polarity_udf(col("text")))
        .withColumn("textblob_subjectivity", textblob_subjectivity_udf(col("text")))
    )

    # ============================================================
    # Write FINAL collection
    # ============================================================
    (
        final_df
        .select(
            "_id",
            "comment_id",
            "post_id",
            "text",
            "score",
            "created_at",
            "sentiment_vivekn",
            "vader_sentiment",
            "vader_confidence",
            "textblob_polarity",
            "textblob_subjectivity"
        )
        .write
        .format("mongodb")
        .mode("append")
        .option("spark.mongodb.connection.uri", "mongodb://mongodb:27017")
        .option("spark.mongodb.database", "reddit_db")
        .option("spark.mongodb.collection", "comments_analysed")
        .save()
    )

    spark.stop()


if __name__ == "__main__":
    main()
