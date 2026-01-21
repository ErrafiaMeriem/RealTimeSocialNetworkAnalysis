from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


# ============================================================
# Dataset PRODUIT par reddit_spark_pipeline
# ============================================================
reddit_kafka_dataset = Dataset("dataset://reddit/kafka/ingested")


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="reddit_sentiment_analysis_dag",
    start_date=days_ago(1),
    schedule=[reddit_kafka_dataset],   # 🔑 DATASET-DRIVEN
    catchup=False,
    default_args=default_args,
    tags=["reddit", "spark", "sentiment"],
) as dag:

    run_sentiment_analysis = BashOperator(
        task_id="run_sentiment_analysis_3models",

        bash_command="""
        docker exec spark-master \
          /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --jars /opt/spark/jars/spark-nlp_2.12-5.2.2.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.796.jar \
          --conf spark.jsl.settings.pretrained.offline=true \
          --conf spark.jsl.settings.pretrained.cache_folder=/opt/spark-nlp-models \
          /opt/spark_jobs/sentiment_analysis_3models.py
        """,
    )
