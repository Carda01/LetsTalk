import json, os, tempfile, logging, requests
from datetime import datetime
from lib.new_utils import get_spark_and_path, get_null_percentage

from pyspark.sql.types import *

LEAGUES_API_URL = "https://v3.football.api-sports.io/leagues"
HEADERS = {
    'x-rapidapi-host': "v3.football.api-sports.io",
}

is_gcs_enabled = os.getenv('IS_GCS_ENABLED')

try:
    api_key = os.getenv('API_KEY')
    HEADERS['x-rapidapi-key'] = api_key
    logging.info("Fetching leagues data from Sports API")
    response_leagues = requests.get(LEAGUES_API_URL, headers=HEADERS)

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

    matches_data = response_matches.json().get("response", [])
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
        json.dump(matches_data, temp_file)
        matches_file_path = temp_file.name
    
    num_matches = len(matches_data)
    matches_metadata = {"inserted_rows": num_matches}
    
    logging.info(f"Fetched {num_leagues} leagues and {num_matches} matches")
    
    fetched_data_info = {
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

spark, delta_table_base_path = get_spark_and_path(is_gcs_enabled)
base_path = os.path.join(delta_table_base_path, "letstalk_landing_zone_bdma/delta_sports")

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
