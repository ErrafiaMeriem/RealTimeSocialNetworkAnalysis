from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.datasets import Dataset
from datetime import timedelta

from scrap_reddit import scrape
from kafka_utils.producer import send_posts, send_comments


comments_dataset = Dataset("kafka://reddit/comments")
posts_dataset = Dataset("kafka://reddit/posts")


def scrape_and_stream(**context):
    posts, comments = scrape()

    emitted = []

    if posts:
        send_posts(posts)
        emitted.append(posts_dataset)

    if comments:
        send_comments(comments)
        emitted.append(comments_dataset)

    # This is the key: return datasets dynamically
    return emitted


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
        outlets=[comments_dataset, posts_dataset],  # declared
    )
