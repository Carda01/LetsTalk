import datetime
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

is_gcs_enabled = Variable.get("is_gcs_enabled", "False")
conn = BaseHook.get_connection('news_api')
api_key = conn.password
lib = "/Users/alfio/projects/upc/BDMP2/dags/lib/"


with DAG(
    dag_id='news_dag_docker',
    start_date=datetime(2025, 3, 1),
    description='A dag that fetches data from the NewsAPI and loads them into Delta Lake',
    schedule_interval="@daily",
    catchup=False
    ) as dag:


    fetch_ingest_news = DockerOperator(
        task_id="fetch_ingest_news",
        image="bdma_spark:1.0",
        command="/script/news_ingestor_script.py",
        environment={"API_KEY": api_key, "IS_GCS_ENABLED": is_gcs_enabled},
        mounts=[Mount(source=lib, target='/script/', type='bind')],
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove='success',
        mount_tmp_dir=False,
    )

    fetch_ingest_news
