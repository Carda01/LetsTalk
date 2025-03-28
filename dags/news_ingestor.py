import json, datetime, os, tempfile, logging
from datetime import datetime
from newsapi import NewsApiClient
from lib.utils import get_spark_and_path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
CATEGORIES = ['entertainment', 'sports', 'technology']

def fetch_from_news_api(**kwargs):
    try:
        conn = BaseHook.get_connection('news_api')
        api_key = conn.password
        logging.info(f"Initializing News API client, with key {api_key[:4]}...")
        newsapi = NewsApiClient(api_key)
        temp_file_paths = {}
        for category in CATEGORIES: 
            logging.info(f"Fetching from News API, category: {category}")
            top_headlines = newsapi.get_top_headlines(page_size=100,
                                                      category=category,
                                                      language='en')

            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
                json.dump(top_headlines.get("articles"), temp_file)
                temp_file_paths[category] = temp_file.name

        return temp_file_paths

    except Exception as e:
        logging.error(f"Error while fetching: {str(e)}")
        raise



def ingest_news(**kwargs):
    temp_file_paths = kwargs['ti'].xcom_pull(task_ids=f'fetch_from_news_api')

    spark, delta_table_base_path = get_spark_and_path()
    delta_table_base_path += "/delta_news"

    for category, tmp_json_path in temp_file_paths.items():
        df = spark.read.json(tmp_json_path)

        delta_table_path = delta_table_base_path + f"/{category}"
        os.makedirs(os.path.dirname(delta_table_path), exist_ok=True)

        df.write.mode("append").format("delta").save(delta_table_path)
        logging.info(f"Writing to Delta Lake at {delta_table_path}")

        os.unlink(tmp_json_path)

    spark.stop()
    logging.info("Spark session stopped successfully")


dag = DAG(
    dag_id='news_dag',
    start_date=datetime(2025, 3, 1),
    description='A dag that fetches data from the NewsAPI and loads them into Delta Lake',
    schedule_interval="@daily",
    catchup=False
)


"""
Operators and flow definition
"""

fetch_news_task = PythonOperator(
    task_id='fetch_from_news_api',
    python_callable=fetch_from_news_api,
    provide_context=True,
    dag=dag
)

load_news_task = PythonOperator(
    task_id='load_news_to_delta_lake',
    python_callable=ingest_news,
    provide_context=True,
    dag=dag
)

fetch_news_task >> load_news_task
