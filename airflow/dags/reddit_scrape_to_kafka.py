from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.datasets import Dataset
from datetime import timedelta

from scrap_reddit import scrape
from kafka_utils.producer import send_posts, send_comments

reddit_kafka_dataset = Dataset("kafka://reddit/comments")

def scrape_and_stream():
    posts, comments = scrape()

    if posts:
        send_posts(posts)

    if comments:
        send_comments(comments)

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="reddit_ingestion_dag",
    start_date=days_ago(1),
    schedule_interval="*/20 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["reddit", "kafka"],
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_reddit_and_send_to_kafka",
        python_callable=scrape_and_stream,
        outlets=[reddit_kafka_dataset],   # 🔴 THIS IS THE KEY
    )
