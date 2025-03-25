import json, datetime, os, tempfile, pyspark, logging
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from newsapi import NewsApiClient

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

def create_spark_session():
    builder = pyspark.sql.SparkSession.builder.appName("LetsTalk") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def fetch_from_news_api(**kwargs):
    try:
        conn = BaseHook.get_connection('news_api')
        api_key = conn.password
        logging.info(f"Initializing News API client, with key {api_key[:4]}...")
        newsapi = NewsApiClient(api_key)


        logging.info(f"Fetching from News API, category entertainment")
        top_headlines = newsapi.get_top_headlines(page_size=100,
                                                  category='entertainment',
                                                  language='en')

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
            json.dump(top_headlines, temp_file)
            temp_file_path = temp_file.name

        return temp_file_path

    except Exception as e:
        logging.error(f"Error while fetching: {str(e)}")
        raise


def ingest_news(**kwargs):
    temp_file_path = kwargs['task_instance'].xcom_pull(task_ids=f'fetch_from_news_api')
    spark = create_spark_session()

    df = spark.read.json(temp_file_path)

    delta_table_path = "/data/delta_news/entertainment"
    os.makedirs(os.path.dirname(delta_table_path), exist_ok=True)

    try:
        DeltaTable.forPath(spark, delta_table_path)
        logging.info(f"Writing to Delta Lake at {delta_table_path}")
    except:
        logging.error(f"Creating Delta Lake table at {delta_table_path}")
        df.write.format("delta").save(delta_table_path)

    spark.stop()
    logging.info("Spark session stopped")

    os.unlink(temp_file_path)
    return delta_table_path

dag = DAG(
    dag_id='news_dag',
    start_date=datetime(2025, 3, 1),
    description='A dag that fetches data from the NewsAPI and loads them into Delta Lake',
    schedule_interval="@daily",
    catchup=False
)

