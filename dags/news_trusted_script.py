import os
from lib.pt_utils import *
from lib.IncrementalLoader import IncrementalLoader
from lib.Processer import NewsProcessor

is_gcs_enabled = os.getenv('IS_GCS_ENABLED')
if is_gcs_enabled.lower() == 'true':
    is_gcs_enabled = True
else:
    is_gcs_enabled = False

spark, base_path = get_spark_and_path(is_gcs_enabled)

landing_path = get_landing_path(base_path)
trusted_path = get_trusted_path(base_path)



CATEGORIES = ['entertainment', 'sports', 'technology']
for category in CATEGORIES:
    logging.info(f"Processing category {category}")
    table_subpath = f'delta_news/{category}'
    loader = IncrementalLoader(spark, landing_path, table_subpath, is_gcs_enabled)
    df = loader.get_new_data()

    processor = NewsProcessor(spark, df, is_gcs_enabled)

    logging.info(processor.df.show(3))
    logging.info(f"Processing {processor.df.count()} elements")
    processor.ensure_schema()
    processor.remove_clear_duplicates()
    processor.name_to_id()
    processor.remove_hidden_duplicates(['url'], ['publishedAt'])
    processor.normalize_text(['title', 'description', 'content'])
    processor.expand_source()
    processor.order_by('publishedAt', ascending=False)

    logging.info("End processing")
    logging.info(processor.df.show(3))

    save_path = os.path.join(trusted_path, table_subpath)
    processor.merge_with_trusted(trusted_path, table_subpath, ['url'])
    loader.update_control_table()


spark.stop()
logging.info("Data was merged")

