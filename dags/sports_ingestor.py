import json, os, tempfile, logging, requests
from datetime import datetime
from lib.utils import get_spark_and_path, get_null_percentage

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from pyspark.sql.types import *
from pyspark.sql.functions import col

LEAGUES_API_URL = "https://v3.football.api-sports.io/leagues"
HEADERS = {
    'x-rapidapi-host': "v3.football.api-sports.io",
}

def fetch_from_sports_api(**kwargs):
    try:
        conn = BaseHook.get_connection('sports_api')
        api_key = conn.password
        HEADERS['x-rapidapi-key'] = api_key
        logging.info("Fetching leagues data from Sports API")
        response_leagues = requests.get(LEAGUES_API_URL, headers=HEADERS)
        # Extract only the main "response" field for relational data storage
        leagues_data = response_leagues.json().get("response", [])
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
            json.dump(leagues_data, temp_file)
            leagues_file_path = temp_file.name
        
        num_leagues = len(leagues_data)
        leagues_metadata = {"inserted_rows": num_leagues}
        
        today = datetime.now().strftime('%Y-%m-%d')
        matches_api_url = f"https://v3.football.api-sports.io/fixtures?date={today}"
        logging.info("Fetching matches data from Sports API")
        response_matches = requests.get(matches_api_url, headers=HEADERS)
        # Extract only the main "response" field for relational data storage
        matches_data = response_matches.json().get("response", [])
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
            json.dump(matches_data, temp_file)
            matches_file_path = temp_file.name
        
        num_matches = len(matches_data)
        matches_metadata = {"inserted_rows": num_matches}
        
        logging.info(f"Fetched {num_leagues} leagues and {num_matches} matches")
        
        return {
            "leagues": {
                "path": leagues_file_path,
                "metadata": leagues_metadata
            },
            "matches": {
                "path": matches_file_path,
                "metadata": matches_metadata
            }
        }
        
    except Exception as e:
        logging.error(f"Error fetching sports data: {str(e)}")
        raise

def ingest_sports_data(**kwargs):
    fetched_data_info = kwargs['ti'].xcom_pull(task_ids='fetch_from_sports_api')
    
    spark, delta_table_base_path = get_spark_and_path()
    base_path = os.path.join(delta_table_base_path, "delta_sports")
    
    for key in fetched_data_info:
        file_path = fetched_data_info[key]['path']
        metadata = fetched_data_info[key]['metadata']
        delta_table_path = os.path.join(base_path, key)
        os.makedirs(os.path.dirname(delta_table_path), exist_ok=True)
        
        df = spark.read.json(file_path)
        
        logging.info(f"Sample data for {key}:")
        df.show(5)

        metadata["perc_rows_inserted_with_null"] = get_null_percentage(df)
        if key == "matches":
            df = df.drop('score')

        df.write.mode("append") \
            .format("delta") \
            .option("mergeSchema", "true") \
            .option("userMetadata", json.dumps(metadata)) \
            .save(delta_table_path)
        
        logging.info(f"Written {key} data to Delta Lake at {delta_table_path}")
        os.unlink(file_path)
    
    spark.stop()
    logging.info("Spark session stopped successfully")

dag = DAG(
    dag_id='sports_api_ingestor_dag',
    start_date=datetime(2025, 3, 1),
    description='A DAG that fetches leagues and matches from the Sports API and loads them into Delta Lake',
    schedule_interval="0 */6 * * *",  # Every 6 hours
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

# This part is for testing the functions independently outside of Airflow if needed.
"""
if __name__ == "__main__":
    result = fetch_from_sports_api()
    print("Fetch result:", result)
    
    class DummyTaskInstance:
        def xcom_pull(self, task_ids):
            return result

    context = {'ti': DummyTaskInstance()}
    ingest_sports_data(**context)
    print("Ingest result: Completed successfully")
"""
