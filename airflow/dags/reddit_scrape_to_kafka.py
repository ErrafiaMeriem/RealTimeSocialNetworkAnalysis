from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

from scrap_reddit import scrape
from kafka_utils.producer import send_posts, send_comments

logger = logging.getLogger(__name__)

def scrape_and_stream():
    try:
        posts, comments = scrape()

        logger.info(f"Posts scraped: {len(posts)}")
        logger.info(f"Comments scraped: {len(comments)}")

        if posts:
            logger.info("Sending posts to Kafka")
            send_posts(posts)

        if comments:
            logger.info("Sending comments to Kafka")
            send_comments(comments)

    except Exception:
        logger.exception("Error in scrape_and_stream")
        raise

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20)
}

with DAG(
    dag_id="reddit_ingestion_dag",
    default_args=default_args,
    schedule_interval="*/20 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["reddit", "scraping", "kafka"]
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_reddit_and_send_to_kafka",
        python_callable=scrape_and_stream,
        dag=dag
    )
