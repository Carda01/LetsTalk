from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
import os
import json
import logging
import pyspark
from delta import *

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'first dag',
    default_args=default_args,
    description='A dag that generates data and load it into Delta Lake',
    schedule_interval=timedelta(days=1),
    catchup=False
)

def create_spark_session():
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def fetch_from_api(**kwargs):
    try:
        api_url = "https://your-api-endpoint.com/data"
        
        headers = {
            "Authorization": f"Bearer {os.environ.get('API_TOKEN', 'default_token')}"
        }
        
        params = {
            "date": kwargs['execution_date'].strftime('%Y-%m-%d')
        }
        
        # For testing: Create sample data instead of making an actual API call
        sample_data = [
            {"id": 1, "name": "Product A", "value": 100, "date": kwargs['execution_date'].strftime('%Y-%m-%d')},
            {"id": 2, "name": "Product B", "value": 200, "date": kwargs['execution_date'].strftime('%Y-%m-%d')},
            {"id": 3, "name": "Product C", "value": 300, "date": kwargs['execution_date'].strftime('%Y-%m-%d')}
        ]
        
        temp_json_path = f"/tmp/api_data_{kwargs['execution_date'].strftime('%Y%m%d')}.json"
        with open(temp_json_path, 'w') as f:
            json.dump(sample_data, f)
        
        # Uncomment for actual API request:
        # response = requests.get(api_url, headers=headers, params=params)
        # if response.status_code == 200:
        #     data = response.json()
        #     temp_json_path = f"/tmp/api_data_{kwargs['execution_date'].strftime('%Y%m%d')}.json"
        #     with open(temp_json_path, 'w') as f:
        #         json.dump(data, f)
        # else:
        #     raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
        
        return temp_json_path
        
    except Exception as e:
        logging.error(f"Error in fetch_from_api: {str(e)}")
        raise


def load_to_delta_lake(**kwargs):
    try:
        ti = kwargs['ti']
        temp_json_path = ti.xcom_pull(task_ids='fetch_from_api')
        
        logging.info(f"Loading data from {temp_json_path} to Delta Lake")
        spark = create_spark_session()
        
        df = spark.read.json(temp_json_path)
        
        delta_table_path = "/data/delta_tables/api_data"
        os.makedirs(os.path.dirname(delta_table_path), exist_ok=True)
        
        logging.info(f"Writing to Delta Lake at {delta_table_path}")
        df.write.format("delta").mode("overwrite").save(delta_table_path)
        
        spark.stop()
        logging.info("Spark session stopped")
        
        return delta_table_path
        
    except Exception as e:
        logging.error(f"Error in load_to_delta_lake: {str(e)}")
        raise


"""
Operators and flow definition
"""

fetch_task = PythonOperator(
    task_id='fetch_from_api',
    python_callable=fetch_from_api,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_delta_lake',
    python_callable=load_to_delta_lake,
    provide_context=True,
    dag=dag
)

fetch_task >> load_task
