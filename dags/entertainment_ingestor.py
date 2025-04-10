import json, datetime, os, tempfile, logging, requests
from datetime import datetime
from pyspark.sql.functions import current_timestamp

from lib.utils import get_spark_and_path, get_null_percentage
from airflow import DAG
from pyspark.sql.functions import lit
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook


def fetch_from_tmdb_api(**kwargs):
    try:
        endpoints_to_query = {
                "upcoming": "/movie/upcoming?language=en-US&page=1",
                "trending": "/trending/movie/week?language=en-US",
                "now_playing": "/movie/now_playing?language=en-US&page=1"
                }
        conn = BaseHook.get_connection('tmdb_api')
        api_key = conn.password
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        logging.info(f"Requesting with access token: {api_key[:4]}...")
        base_url = "https://api.themoviedb.org/3"
        fetched_data_info = {}

        for key, endpoint in endpoints_to_query.items():
            request_url = base_url + endpoint

            logging.info(f"Fetching {key} movies from TMDB API")
            movies = requests.get(request_url, headers=headers).json()

            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
                json.dump(movies.get("results"), temp_file)
                fetched_data_info[key] = {}
                fetched_data_info[key]['path'] = temp_file.name
                if key in ['upcoming', 'now_playing']:
                    fetched_data_info[key]['validity_dates'] = movies.get("dates")

        return fetched_data_info


    except Exception as e:
        logging.error(f"Error while fetching: {str(e)}")
        raise


def ingest_tmdb(**kwargs):
    fetched_data_info = kwargs['ti'].xcom_pull(task_ids=f'fetch_from_tmdb_api')

    spark, delta_table_base_path = get_spark_and_path()
    delta_table_base_path += "/delta_tmdb"

    for category, fetched_info in fetched_data_info.items():
        df = spark.read.json(fetched_info['path'])

        delta_table_path = delta_table_base_path + f"/{category}"
        os.makedirs(os.path.dirname(delta_table_path), exist_ok=True)
        metadata = {}
        metadata["perc_rows_inserted_with_null"] = get_null_percentage(df)

        logging.info(f"Adding new column with timestamps")
        df = df.withColumn("ingestion_time", current_timestamp())
        if category in ['upcoming', 'now_playing']:
            df = df.withColumn("begin_date", lit(fetched_info['validity_dates']['minimum'])).withColumn("end_date", lit(fetched_info['validity_dates']['maximum']))
        df.write.mode("append") \
            .format("delta") \
            .option("userMetadata", json.dumps(metadata)) \
            .save(delta_table_path)
        logging.info(f"Writing to Delta Lake at {delta_table_path}")

        os.unlink(fetched_info['path'])

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
