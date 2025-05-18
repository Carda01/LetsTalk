import os
from lib.new_utils import *
from lib.IncrementalLoader import IncrementalLoader
from lib.Processer import NewsProcessor

is_gcs_enabled = os.getenv('IS_GCS_ENABLED')

spark, base_path = get_spark_and_path(is_gcs_enabled)

landing_path = get_landing_path(base_path)
trusted_path = get_trusted_path(base_path)

table_subpath = 'delta_news/sports'
loader = IncrementalLoader(spark, landing_path, table_subpath)
df = loader.get_new_data()

processor = NewsProcessor(spark, df)

logging.info("Begin processing")
logging.info(processor.df.head(3))
processor.ensure_schema()
processor.remove_clear_duplicates()
processor.name_to_id()
processor.remove_hidden_duplicates(['url'], ['publishedAt'])
processor.normalize_text(['title', 'description', 'content'])
processor.expand_source()
processor.order_by('publishedAt', ascending=False)

logging.info("End processing")
logging.info(processor.df.head(3))

save_path = os.path.join(trusted_path, table_subpath)
processor.merge_with_trusted(save_path, ['url'])

logging.info("Data was merged")

