from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.datasets import Dataset
from datetime import timedelta


# ============================================================
# Dataset consommé (source Kafka)
# ============================================================
kafka_comments_dataset = Dataset("kafka://reddit/comments")

# ============================================================
# Dataset PRODUIT par ce DAG
# ============================================================
ingested_dataset = Dataset("dataset://reddit/kafka/ingested")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="reddit_spark_pipeline",
    start_date=days_ago(1),
    schedule=[kafka_comments_dataset],   # 🔑 déclenché par Kafka
    catchup=False,
    default_args=default_args,
    tags=["spark", "standalone"],
) as dag:

    spark_transform_store = SparkSubmitOperator(
        task_id="spark_transform_store",

        application="/opt/spark_jobs/reddit_transform_store.py",
        spark_binary="/opt/spark/bin/spark-submit",

        conn_id="spark_standalone",

        # 🔥 PUBLICATION DU DATASET
        outlets=[ingested_dataset],

        verbose=True,
    )
