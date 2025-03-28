import json, datetime, os, tempfile, logging, requests
from datetime import datetime

from lib.utils import get_spark_and_path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook


def fetch_from_tmdb_api(**kwargs):
    try:
        conn = BaseHook.get_connection('tmdb_api')
        api_key = conn.password
        base_url = "https://api.themoviedb.org/3"
        popular_movie_url = base_url + "/movie/popular?language=en-US&page=1"
        logging.info(f"Requesting with access token: {api_key[:4]}...")

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {api_key}"
        }

        logging.info(f"Fetching popular movies from TMDB API")
        pop_movie = requests.get(popular_movie_url, headers=headers).json()

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
            json.dump(pop_movie.get("results"), temp_file)
            temp_file_path = temp_file.name

        return {"popular": temp_file_path}

    except Exception as e:
        logging.error(f"Error while fetching: {str(e)}")
        raise



def ingest_tmdb(**kwargs):
    temp_file_paths = kwargs['ti'].xcom_pull(task_ids=f'fetch_from_tmdb_api')

    spark, delta_table_base_path = get_spark_and_path()
    delta_table_base_path += "/delta_tmdb"

    for category, tmp_json_path in temp_file_paths.items():
        df = spark.read.json(tmp_json_path)

        delta_table_path = delta_table_base_path + f"/{category}"
        os.makedirs(os.path.dirname(delta_table_path), exist_ok=True)

        df.write.mode("overwrite").format("delta").save(delta_table_path)
        logging.info(f"Writing to Delta Lake at {delta_table_path}")

        os.unlink(tmp_json_path)

    spark.stop()
    logging.info("Spark session stopped successfully")


dag = DAG(
    dag_id='movie_dag',
    start_date=datetime(2025, 3, 1),
    description='A dag that fetches data from IMDB and loads them into Delta Lake',
    schedule_interval="@weekly",
    catchup=False
)


"""
Operators and flow definition
"""

fetch_tmdb_task = PythonOperator(
    task_id='fetch_from_tmdb_api',
    python_callable=fetch_from_tmdb_api,
    provide_context=True,
    dag=dag
)

load_tmdb_task = PythonOperator(
    task_id='load_tmdb_to_delta_lake',
    python_callable=ingest_tmdb,
    provide_context=True,
    dag=dag
)

fetch_tmdb_task >> load_tmdb_task
