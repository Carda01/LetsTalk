import os
from lib.pt_utils import *
from lib.IncrementalLoader import IncrementalLoader
from pyspark.sql.utils import AnalysisException
from delta import *
from lib.Processer import *

is_gcs_enabled = os.getenv('IS_GCS_ENABLED')
if is_gcs_enabled.lower() == 'true':
    is_gcs_enabled = True
else:
    is_gcs_enabled = False

spark, base_path = get_spark_and_path(is_gcs_enabled)

landing_path = get_landing_path(base_path)
trusted_path = get_trusted_path(base_path)

if not path_exists(trusted_path, "movie", is_gcs_enabled):
    print("This is the first time TMDB is ingested. Starting initialization")
    db_path = os.path.join(landing_path, 'delta_tmdb/database')

    movies = spark.read.format("delta").load(os.path.join(db_path, 'movie'))
    genres = spark.read.format("delta").load(os.path.join(db_path, 'genre'))
    movie_genre = spark.read.format("delta").load(os.path.join(db_path, 'movie_genre'))

    processor = TMDBProcessor(spark, movies, is_gcs_enabled)

    processor.ensure_schema()
    processor.normalize_text(['title', 'overview'])
    processor.remove_clear_duplicates()
    processor.remove_hidden_duplicates(['film_id'], ['ingestion_time'], True)

    processor.set_genre_df(genres)
    processor.ensure_schema_genres()

    processor.genre_df = processor.genre_df.withColumn("genre", lower(col("genre")))
    processor.genre_df = processor.genre_df.withColumn("genre", regexp_replace(col("genre"), r"http\S+|www\.\S+", " "))
    processor.genre_df = processor.genre_df.withColumn("genre", regexp_replace(col("genre"), r"[^a-zA-Z\s]", " "))
    processor.genre_df.dropDuplicates()
    window = Window.partitionBy(*["genre"]).orderBy(*["genre_id"])
    processor.genre_df= (
            processor.genre_df.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1).drop("row_num")
            )

    processor.set_movie_genre_df(movie_genre)
    processor.ensure_schema_movie_genres()
    processor.movie_genre_df.dropDuplicates()

    processor.static_dump(os.path.join(trusted_path, 'delta_tmdb'))

##### API #####
CATEGORIES = ['now_playing', 'trending', 'upcoming']
loaders = []
dfs = {}

processor = None
for category in CATEGORIES:
    logging.info(f"Loading {category}")
    table_subpath = f'delta_tmdb/{category}'

    loader = IncrementalLoader(spark, landing_path, table_subpath, is_gcs_enabled)
    df = loader.get_new_data()
    if df.isEmpty():
        logging.info(f"No data found for {category}")
        continue
    dfs[category] = df
    loaders.append(loader)


if dfs:
    processors = []
    for name, df in dfs.items():
        processor = TMDBProcessor(spark, df, is_gcs_enabled)
        processors.append(processor)
        processor.ensure_schema()
        processor.normalize_text(['title', 'overview'])
        processor.type_dump(os.path.join(trusted_path, 'delta_tmdb'), name)
        processor.ensure_schema_movie_genres()

    primary_processor = processors[0]
    for i in range(1, len(processors)):
        primary_processor.combine_dfs(processors[i])

    primary_processor.remove_clear_duplicates()
    primary_processor.remove_hidden_duplicates(['film_id'], ['ingestion_time'], True)
    primary_processor.merge_with_trusted(trusted_path)


for loader in loaders:
    loader.update_control_table()

spark.stop()
logging.info("Data was merged")