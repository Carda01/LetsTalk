import os, logging
from lib.utils import get_spark_and_path
from datetime import datetime
import kagglehub
from kagglehub import KaggleDatasetAdapter

from airflow import DAG
from airflow.operators.python import PythonOperator

TABLES = ["MOVIE", "GENRE", "MOVIE_GENRE"]

def fetch_and_upload(**kwargs):
    spark, delta_table_base_path = get_spark_and_path()
    delta_table_base_path += "/delta_tmdb"

    for table in TABLES:
        delta_table_path = delta_table_base_path + f"/{table.lower()}"
        logging.info(f"Downloading table {table.lower()}")
        df = kagglehub.dataset_load(
                KaggleDatasetAdapter.PANDAS,
                "omercolakoglu/tmdb-website-movie-database",
                f"{table}.xlsx")
        df = spark.createDataFrame(df)
        os.makedirs(os.path.dirname(delta_table_path), exist_ok=True)
        logging.info(f"Writing to Delta Lake at {delta_table_path}")
        df.write.mode("overwrite").format("delta").save(delta_table_path)


    spark.stop()
    logging.info("Spark session stopped successfully")

    
dag = DAG(
    dag_id='movie_dataset_upload_dag',
    start_date=datetime(2025, 3, 1),
    description='A dag that fetches data from Kaggle and loads them into Delta Lake',
    schedule_interval="@once",
    catchup=False
)


"""
Operators and flow definition
"""

fetch_and_upload_task = PythonOperator(
    task_id='fetch_and_upload',
    python_callable=fetch_and_upload,
    provide_context=True,
    dag=dag
)

fetch_and_upload_task
