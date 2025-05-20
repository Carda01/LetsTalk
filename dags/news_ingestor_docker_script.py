import json, os, tempfile, logging
from newsapi import NewsApiClient
from lib.pt_utils import get_spark_and_path, get_null_percentage

CATEGORIES = ['entertainment', 'sports', 'technology']

api_key = os.getenv('API_KEY')
is_gcs_enabled = os.getenv('IS_GCS_ENABLED')

logging.info(f"Initializing News API client, with key {api_key[:4]}...")
newsapi = NewsApiClient(api_key)
fetched_data_info = {}
for category in CATEGORIES:
    logging.info(f"Fetching from News API, category: {category}")
    top_headlines = newsapi.get_top_headlines(page_size=100,
                                              category=category,
                                              language='en')

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
        json.dump(top_headlines.get("articles"), temp_file)
        fetched_data_info[category] = {}
        fetched_data_info[category]['path'] = temp_file.name
        fetched_data_info[category]['metadata'] = {"inserted_rows": top_headlines.get('totalResults')}

logging.info(os.listdir())
spark, delta_table_base_path = get_spark_and_path(is_gcs_enabled)
delta_table_base_path += "/letstalk_landing_zone_bdma/delta_news"

for category, fetched_info in fetched_data_info.items():
    df = spark.read.json(fetched_info['path'])
    metadata = fetched_info['metadata']
    metadata["perc_rows_inserted_with_null"] = get_null_percentage(df)

    delta_table_path = delta_table_base_path + f"/{category}"
    os.makedirs(os.path.dirname(delta_table_path), exist_ok=True)

    df.write.mode("append") \
        .format("delta") \
        .option("userMetadata", json.dumps(metadata)) \
        .save(delta_table_path)
    logging.info(f"Writing to Delta Lake at {delta_table_path}")

    os.unlink(fetched_info['path'])

spark.stop()
logging.info("Spark session stopped successfully")
