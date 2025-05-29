from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from docker.types import Mount
from airflow.models import Variable
import os

is_gcs_enabled = Variable.get("is_gcs_enabled", "False")
base_path = "/Users/alfio/projects/upc/BDMP2/"
dags = os.path.join(base_path, 'dags')
data = os.path.join(base_path, 'data')

with DAG(
    dag_id="sports_to_trusted_dag",
    start_date=datetime(2025, 5, 1),
    description='A dag that moves data from the landing to the trusted zone applying some processing for the sports data',
    schedule_interval="@daily",
    catchup=False
) as dag:

    sports_to_trusted_migrator = DockerOperator(
        task_id="sports_to_trusted_migrator",
        image="bdma_spark:1.0",
        command="/script/sports_trusted_script.py",
        environment={"IS_GCS_ENABLED": is_gcs_enabled},
        mounts=[Mount(source=dags, target='/script/', type='bind'), Mount(source=data, target='/data/', type='bind')],
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove='success',
        mount_tmp_dir=False,
    )

    sports_to_trusted_migrator
