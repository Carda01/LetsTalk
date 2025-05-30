import os
from lib.pt_utils import *
from lib.IncrementalLoader import IncrementalLoader
from lib.Processer import SportsLeagueProcessor, SportsMatchesProcessor

is_gcs_enabled = os.getenv('IS_GCS_ENABLED')
if is_gcs_enabled.lower() == 'true':
    is_gcs_enabled = True
else:
    is_gcs_enabled = False

spark, base_path = get_spark_and_path(is_gcs_enabled)

landing_path = get_landing_path(base_path)
trusted_path = get_trusted_path(base_path)

######################## LEAGUES ############################
logging.info("Processing leagues")
table_subpath = f'delta_sports/leagues'
loader = IncrementalLoader(spark, landing_path, table_subpath, is_gcs_enabled)
df = loader.get_new_data()
if df.isEmpty():
    logging.info(f"No new data for {table_subpath}")
else:
    processor = SportsLeagueProcessor(spark, df, is_gcs_enabled)

    logging.info(processor.df.show(3))
    logging.info(f"Processing {processor.df.count()} elements")
    processor.ensure_schema()
    processor.remove_clear_duplicates()
    processor.generate_leagues()
    processor.generate_countries()

    logging.info("End processing")
    logging.info(processor.df.show(3))

    processor.merge_with_trusted(trusted_path, 'delta_sports')
    loader.update_control_table()

######################## MATCHES ############################

logging.info("Processing matches")
table_subpath = f'delta_sports/matches'
loader = IncrementalLoader(spark, landing_path, table_subpath, is_gcs_enabled)
df = loader.get_new_data()

if df.isEmpty():
    logging.info(f"No new data for {table_subpath}")
else:
    processor = SportsMatchesProcessor(spark, df, is_gcs_enabled)
    logging.info(processor.df.show(3))
    logging.info(f"Processing {processor.df.count()} elements")

    processor.expand()
    processor.normalize_text(['referee', 'venue_city', 'venue_name', 'team_away_name', 'team_home_name', 'status_long'])
    processor.extract_teams()
    processor.extract_venues()
    processor.remove_useless_columns()
    processor.ensure_schema()
    processor.remove_clear_duplicates()
    processor.remove_hidden_duplicates(['fixture_id', 'status_long'])

    logging.info("End processing")
    logging.info(processor.df.show(3))

    processor.merge_with_trusted(trusted_path, 'delta_sports', ['fixture_id', 'status_long'])
    loader.update_control_table()

spark.stop()
logging.info("Data was merged")

