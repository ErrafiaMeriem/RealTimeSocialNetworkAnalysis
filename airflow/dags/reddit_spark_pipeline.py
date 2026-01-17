from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from docker.types import Mount
from datetime import timedelta


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="reddit_spark_pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    tags=["spark", "mongo"]
) as dag:

    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_reddit_ingestion",
        external_dag_id="reddit_ingestion_dag",
        external_task_id="scrape_reddit_and_send_to_kafka",
        allowed_states=["success"],
        mode="reschedule",
        poke_interval=60,
        timeout=3600,
    )

    spark_job = DockerOperator(
        task_id="spark_transform_store",
        image="apache/spark:3.5.3",
        api_version="auto",
        auto_remove=True,
        command="""
        /opt/spark/bin/spark-submit
        --master spark://spark-master:7077
        /opt/project/spark_jobs/reddit_transform_store.py
        """,
        docker_url="unix://var/run/docker.sock",
        network_mode="bigdata-net",
        mounts=[
            Mount(
                source="/spark_jobs",
                target="/opt/project",
                type="bind",
            ),
        ],
    )

    wait_for_ingestion >> spark_job
