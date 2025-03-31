import json, os, tempfile, logging
import requests
from datetime import datetime
from lib.utils import get_spark_and_path, get_null_percentage

from airflow import DAG
from airflow.operators.python import PythonOperator

API_URL = "https://v3.football.api-sports.io/leagues"
HEADERS = {
    'x-rapidapi-host': "v3.football.api-sports.io",
    'x-rapidapi-key': "dc72c9cd0123f09af49faf572c34f3cf"  
    }

def fetch_from_sports_api(**kwargs):
    """
    Fetch data from the Sports API and write it to a temporary JSON file.
    Returns a dict with file path and some metadata.
    """
    try:
        logging.info("Fetching leagues data from Sports API")
        response = requests.get(API_URL, headers=HEADERS)
        data = response.json()
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
            json.dump(data, temp_file)
            temp_file_path = temp_file.name

        num_records = len(data.get("response", []))
        metadata = {"inserted_rows": num_records}

        logging.info(f"Fetched {num_records} records; data stored at {temp_file_path}")
        return {"path": temp_file_path, "metadata": metadata}

    except Exception as e:
        logging.error(f"Error fetching sports data: {str(e)}")
        raise

def ingest_sports_data(**kwargs):
    """
    Read the temporary JSON file, load it into a Spark DataFrame,
    compute some metadata (like percentage of nulls), and write to Delta Lake.
    """
    fetched_data_info = kwargs['ti'].xcom_pull(task_ids='fetch_from_sports_api')
    
    spark, delta_table_base_path = get_spark_and_path()
    delta_table_path = os.path.join(delta_table_base_path, "delta_sports", "leagues")
    os.makedirs(os.path.dirname(delta_table_path), exist_ok=True)
    
    df = spark.read.json(fetched_data_info['path'])
    metadata = fetched_data_info['metadata']
    metadata["perc_rows_inserted_with_null"] = get_null_percentage(df)
    
    df.write.mode("append") \
        .format("delta") \
        .option("userMetadata", json.dumps(metadata)) \
        .save(delta_table_path)
    
    logging.info(f"Written sports data to Delta Lake at {delta_table_path}")
    
    os.unlink(fetched_data_info['path'])
    
    spark.stop()
    logging.info("Spark session stopped successfully")

dag = DAG(
    dag_id='sports_api_ingestor_dag',
    start_date=datetime(2025, 3, 1),
    description='A DAG that fetches data from the Sports API and loads it into Delta Lake',
    schedule_interval="@daily",
    catchup=False
)

fetch_sports_task = PythonOperator(
    task_id='fetch_from_sports_api',
    python_callable=fetch_from_sports_api,
    provide_context=True,
    dag=dag
)

load_sports_task = PythonOperator(
    task_id='load_sports_to_delta_lake',
    python_callable=ingest_sports_data,
    provide_context=True,
    dag=dag
)

fetch_sports_task >> load_sports_task




# For local testing, you can run the following code block to test the functions independently.
"""
if __name__ == "__main__":
    # First, test the fetch function
    result = fetch_from_sports_api()
    print("Fetch result:", result)
    
    # Create a dummy task instance for testing the ingest function
    class DummyTaskInstance:
        def xcom_pull(self, task_ids):
            return result

    # Build a fake context with our dummy task instance
    context = {'ti': DummyTaskInstance()}
    
    # Run the ingest function using the fake context
    ingest_sports_data(**context)
    print("Ingest result: Completed successfully")

    """