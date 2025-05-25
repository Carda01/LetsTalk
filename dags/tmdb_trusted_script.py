import os
from lib.pt_utils import *
from lib.IncrementalLoader import IncrementalLoader
from pyspark.sql.utils import AnalysisException
from delta import *
from lib.Processer import *

#is_gcs_enabled = os.getenv('IS_GCS_ENABLED')
is_gcs_enabled= "False"
if is_gcs_enabled.lower() == 'true':
    is_gcs_enabled = True
else:
    is_gcs_enabled = False

spark, base_path = get_spark_and_path(is_gcs_enabled)

landing_path = '..\data\letstalk_landing_zone_bdma' #get_landing_path(base_path)
trusted_path ='..\data\letstalk_trusted_zone_bdma'

path= '..\data\letstalk_trusted_zone_bdma\movie'

try:
    delta_table = DeltaTable.forPath(spark, path)
    print("Delta table exists.")
except AnalysisException:
    print("Delta table does not exist. Starting initialization")
    movies_subpath = r'delta_tmdb\database\movie'
    genre_subpath  = r'delta_tmdb\database\genre'
    movies_genre_subpath = r'delta_tmdb\database\movie_genre'

    loader = IncrementalLoader(spark, landing_path, movies_subpath, is_gcs_enabled)
    df = loader.get_new_data()
    loader_genre = IncrementalLoader(spark, landing_path, genre_subpath, is_gcs_enabled)
    df_genre= loader_genre.get_new_data()
    loader_mov_gen = IncrementalLoader(spark, landing_path, movies_genre_subpath, is_gcs_enabled)
    df_mov_gen= loader_mov_gen.get_new_data()

    processor = TMDBProcessor(spark, df, is_gcs_enabled)

    processor.ensure_schema()
    processor.normalize_text(['overview'])
    processor.remove_clear_duplicates()
    processor.remove_hidden_duplicates(['film_id'], ['ingestion_time'], True)

    processor.set_genre_df(df_genre)
    processor.ensure_schema_genres()

    processor.genre_df = processor.genre_df.withColumn("genre", lower(col("genre")))
    processor.genre_df = processor.genre_df.withColumn("genre", regexp_replace(col("genre"), r"http\S+|www\.\S+", " "))
    processor.genre_df = processor.genre_df.withColumn("genre", regexp_replace(col("genre"), r"[^a-zA-Z\s]", " "))
    processor.genre_df.dropDuplicates()
    window = Window.partitionBy(*["genre"]).orderBy(*["genre_id"])
    processor.genre_df= (
            processor.genre_df.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1).drop("row_num")
            )

    processor.set_movie_genre_df(df_mov_gen)
    processor.ensure_schema_movie_genres()
    processor.movie_genre_df.dropDuplicates()

    processor.static_dump(trusted_path)

#api dataset to trusted  - to be repeated
table_subpaths = [r'delta_tmdb\now_playing',r'delta_tmdb\trending',r'delta_tmdb\upcoming']

for i in range(len(table_subpaths)):
    loader = IncrementalLoader(spark, landing_path, table_subpaths[i], is_gcs_enabled)
    df = loader.get_new_data()
    if i==0:
        processor= TMDBProcessor(spark, df, is_gcs_enabled)

        processor.ensure_schema()
        processor.normalize_text(['overview'])
        processor.ensure_schema_movie_genres()
        processor.type_dump(trusted_path, table_subpaths[i])
    
    else:
        processor1= TMDBProcessor(spark, df, is_gcs_enabled)

        processor1.ensure_schema()
        processor1.normalize_text(['overview'])
        processor1.ensure_schema_movie_genres()
        processor1.type_dump(trusted_path, table_subpaths[i])
        processor.combine_dfs(processor1)

processor.remove_clear_duplicates()
processor.remove_hidden_duplicates(['film_id'], ['ingestion_time'], True)
processor.merge_with_trusted(trusted_path)
spark.stop()
logging.info("Data was merged")