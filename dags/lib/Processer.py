import os, sys
from abc import ABC, abstractmethod
from lib.pt_utils import merge_elements, get_logger, gcs_path_exists

from delta import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, lower, regexp_replace, struct, to_timestamp, max as spark_max, lit, \
    coalesce, concat, length, row_number, explode, current_timestamp, from_unixtime
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"

logging = get_logger()

def path_exists(bucket_path, table_path, is_gcs_enabled):
    path = os.path.join(bucket_path, table_path)
    does_path_exists = (not is_gcs_enabled and os.path.exists(path)) or (is_gcs_enabled and gcs_path_exists(bucket_path, table_path))
    logging.info(f"does path {path} exists: {does_path_exists}")
    return does_path_exists


def basic_merge_with_trusted(df, spark, bucket_path, table_path, key_cols, is_gcs_enabled):
    path = os.path.join(bucket_path, table_path)
    if path_exists(bucket_path, table_path, is_gcs_enabled):
        delta_table = DeltaTable.forPath(spark, path)
        data_cols = [c for c in df.columns if c not in key_cols]

        update_expr = {c: coalesce(col(f"overlap.{c}"), col(f"trusted.{c}")) for c in data_cols}
        init_count = delta_table.toDF().count()

        logging.info("Saving unique records from overlapping ones")
        (
            delta_table.alias("trusted")
            .merge(
                df.alias("overlap"),
                " AND ".join([f"trusted.{k}=overlap.{k}" for k in key_cols])
            )
            .whenMatchedUpdate(
                set=update_expr
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        final_count = delta_table.toDF().count()
        logging.info(f"Added new {final_count - init_count} unique records to table at path {path}")

    else:
        df.write.format("delta").mode("append").save(path)
        logging.info(f"Adding new {df.count()} records to table at path {path}")


class Processer(ABC):

    def __init__(self, spark, df, is_gcs_enabled = False):
        self.spark = spark
        self.df = df
        self.is_gcs_enabled = is_gcs_enabled


    @abstractmethod
    def ensure_schema(self):
        pass

    @abstractmethod
    def merge_with_trusted(self, bucket_path, table_path, key_cols):
        pass


    def remove_clear_duplicates(self):
        initial_count = self.df.count()
        self.df = self.df.dropDuplicates()
        final_count = self.df.count()
        logging.info(f"Removed {initial_count - final_count} simple duplicate(s)")


    def remove_hidden_duplicates(self, key_cols, order_cols=None, desc=False):
        initial_count = self.df.count()
        self.df = merge_elements(self.df, key_cols, order_cols, desc)
        final_count = self.df.count()

        logging.info(f"Removed {initial_count - final_count} hidden duplicate(s)")


    def normalize_text(self, columns):
        for column in columns:
            self.df = self.df.withColumn(column, lower(col(column)))
            self.df = self.df.withColumn(column, regexp_replace(col(column), r"http\S+|www\.\S+", " "))
            self.df = self.df.withColumn(column, regexp_replace(col(column), r"[^a-zA-Z\s]", " "))


    def order_by(self, column, ascending=True):
        self.df = self.df.orderBy(column, ascending=ascending)



class NewsProcessor(Processer):
    def __init__(self, spark, df, is_gcs_enabled = False):
        super().__init__(spark, df, is_gcs_enabled)


    def ensure_schema(self):
        self.df = (self.df.withColumn("publishedAt",
                                      to_timestamp(col("publishedAt"), TIMESTAMP_FORMAT)))

        self.df = self.df.filter(col("url").isNotNull())



    def name_to_id(self):
        self.df = self.df.withColumn(
            "source",
            when(
                col("source.id").isNull(),
                struct(
                    regexp_replace(lower(col("source.name")), " ", "-").alias("id"),
                    col("source.name").alias("name")
                )
            ).otherwise(col("source"))
        )


    def expand_source(self):
        self.df = self.df.withColumn("source", col("source.id"))

    def merge_with_trusted(self, bucket_path, table_path, key_cols):
        path = os.path.join(bucket_path, table_path)
        if path_exists(bucket_path, table_path, self.is_gcs_enabled):
            max_ts = (self.spark.read
                      .format("delta")
                      .load(path)
                      .select(spark_max(col("publishedAt")).alias("max_ts"))
                      .collect())[0]["max_ts"]

            df_new = self.df.filter(col("publishedAt") > lit(max_ts))
            df_overlap = self.df.filter(col("publishedAt") <= lit(max_ts))
        
            delta_table = DeltaTable.forPath(self.spark, path)
            data_cols = [c for c in df_overlap.columns if c not in key_cols]

            update_expr = {c: coalesce(col(f"overlap.{c}"), col(f"trusted.{c}")) for c in data_cols}
            init_count = delta_table.toDF().count()

            logging.info("Saving unique records from overlapping ones")
            (
                delta_table.alias("trusted")
                .merge(
                    df_overlap.alias("overlap"),
                    " AND ".join([f"trusted.{k}=overlap.{k}" for k in key_cols])
                )
                .whenMatchedUpdate(
                    set=update_expr
                )
                .whenNotMatchedInsertAll()
                .execute()
            )

            mid_count = delta_table.toDF().count()
            logging.info(f"Added new {mid_count - init_count} unique records")

            logging.info("Appending non overlapping records")
            df_new.write.format("delta").mode("append").save(path)

            logging.info(f"Adding new {df_new.count()} records")

        else:
            self.df.write.format("delta").mode("append").save(path)
            logging.info(f"Adding new {self.df.count()} records")


class SportsMatchesProcessor(Processer):
    def __init__(self, spark, df, is_gcs_enabled = False):
        super().__init__(spark, df, is_gcs_enabled)
        self.teams = None
        self.venues = None

    def ensure_schema(self):
        for column in ['timestamp', 'period_first', 'period_second']:
            self.df = (self.df.withColumn(column,
                                          from_unixtime(col(column), TIMESTAMP_FORMAT)))


    def merge_with_trusted(self, bucket_path, table_path, key_cols):
        basic_merge_with_trusted(self.teams, self.spark, bucket_path, os.path.join(table_path, 'teams'), ['team_id'], self.is_gcs_enabled)
        basic_merge_with_trusted(self.venues, self.spark, bucket_path, os.path.join(table_path, 'venues'), ['venue_id'], self.is_gcs_enabled)

        ### Merge matches ###
        table_path = os.path.join(table_path, 'matches')
        path = os.path.join(bucket_path, table_path)
        if path_exists(bucket_path, table_path, self.is_gcs_enabled):
            max_ts = (self.spark.read
                      .format("delta")
                      .load(path)
                      .select(spark_max(col("timestamp")).alias("max_ts"))
                      .collect())[0]["max_ts"]

            df_new = self.df.filter(col("timestamp") > lit(max_ts))
            df_overlap = self.df.filter(col("timestamp") <= lit(max_ts))

            delta_table = DeltaTable.forPath(self.spark, path)
            data_cols = [c for c in df_overlap.columns if c not in key_cols]

            update_expr = {c: coalesce(col(f"overlap.{c}"), col(f"trusted.{c}")) for c in data_cols}
            init_count = delta_table.toDF().count()

            logging.info("Saving unique records from overlapping ones")
            (
                delta_table.alias("trusted")
                .merge(
                    df_overlap.alias("overlap"),
                    " AND ".join([f"trusted.{k}=overlap.{k}" for k in key_cols])
                )
                .whenMatchedUpdate(
                    set=update_expr
                )
                .whenNotMatchedInsertAll()
                .execute()
            )

            mid_count = delta_table.toDF().count()
            logging.info(f"Added new {mid_count - init_count} unique records")

            logging.info("Appending non overlapping records")
            df_new.write.format("delta").mode("append").save(path)

            logging.info(f"Adding new {df_new.count()} records")

        else:
            self.df.write.format("delta").mode("append").save(path)
            logging.info(f"Adding new {self.df.count()} records")



    def expand(self):
        self.df = (self.df
                   .withColumn("fixture_date", col("fixture.date"))
                   .withColumn("fixture_id", col("fixture.id"))
                   .withColumn("period_first", col("fixture.periods.first"))
                   .withColumn("period_second", col("fixture.periods.second"))
                   .withColumn("referee", col("fixture.referee"))
                   .withColumn("status_elapsed", col("fixture.status.elapsed"))
                   .withColumn("status_extra", col("fixture.status.extra"))
                   .withColumn("status_long", col("fixture.status.long"))
                   .withColumn("status_short", col("fixture.status.short"))
                   .withColumn("timestamp", col("fixture.timestamp"))
                   .withColumn("timezone", col("fixture.timezone"))
                   .withColumn("venue_city", col("fixture.venue.city"))
                   .withColumn("venue_id", col("fixture.venue.id"))
                   .withColumn("venue_name", col("fixture.venue.name"))
                   .withColumn("league", col("league.id"))
                   .drop('fixture')
                   .withColumn("team_away_id", col("teams.away.id"))
                   .withColumn("team_away_logo", col("teams.away.logo"))
                   .withColumn("team_away_name", col("teams.away.name"))
                   .withColumn("team_away_winner", col("teams.away.winner"))
                   .withColumn("team_home_id", col("teams.home.id"))
                   .withColumn("team_home_logo", col("teams.home.logo"))
                   .withColumn("team_home_name", col("teams.home.name"))
                   .withColumn("team_home_winner", col("teams.home.winner"))
                   .drop('teams')
                   .withColumn("goals_away", col("goals.away"))
                   .withColumn("goals_home", col("goals.home"))
                   .drop("goals")
                   .drop("score")
                   )


    def extract_teams(self):
        self.teams = self.df.select('team_away_id', 'team_away_logo', 'team_away_name') \
            .withColumnRenamed('team_away_id', 'team_id') \
            .withColumnRenamed('team_away_logo', 'team_logo') \
            .withColumnRenamed('team_away_name', 'team_name') \
            .union(
            self.df.select('team_home_id', 'team_home_logo', 'team_home_name') \
                .withColumnRenamed('team_home_id', 'team_id') \
                .withColumnRenamed('team_home_logo', 'team_logo') \
                .withColumnRenamed('team_home_name', 'team_name')
        )

        for column in ['team_away_logo', 'team_away_name', 'team_home_logo', 'team_home_name', 'team_home_winner',
                       'team_away_winner']:
            self.df = self.df.drop(column)

        self.teams = self.teams.dropDuplicates()
        self.teams = merge_elements(self.teams, ['team_id'])


    def extract_venues(self):
        self.df = self.df.withColumn(
            "venue_id",
            when(col('venue_name').isNull(), 'unknown')
            .otherwise(regexp_replace(col("venue_name"), ' ', '_')))
        self.venues = self.df.select('venue_id', 'venue_name', 'venue_city')

        for column in ['venue_name', 'venue_city']:
            self.df = self.df.drop(column)

        self.venues = merge_elements(self.venues, ['venue_id'])
        self.venues = self.venues.dropDuplicates()



    def remove_useless_columns(self):
        for column in ['timezone', 'status_short']:
            self.df = self.df.drop(column)


class SportsLeagueProcessor(Processer):
    def __init__(self, spark, df, is_gcs_enabled = False):
        super().__init__(spark, df, is_gcs_enabled)
        self.leagues = None
        self.countries = None


    def ensure_schema(self):
        pass


    def expand(self):
        self.df = (self.df
                   .withColumn('league-info', explode("seasons")).drop("seasons")
                   .withColumn('league-info_current', col('league-info.current'))
                   .withColumn('league-info_end', col('league-info.end'))
                   .withColumn('league-info_start', col('league-info.start'))
                   .withColumn('league-info_year', col('league-info.year'))
                   .withColumn('coverage', col('league-info.coverage'))
                   .drop('league-info')
                   .withColumn('coverage_injuries', col('coverage.injuries'))
                   .withColumn('coverage_odds', col('coverage.odds'))
                   .withColumn('coverage_players', col('coverage.players'))
                   .withColumn('coverage_predictions', col('coverage.predictions'))
                   .withColumn('coverage_standings', col('coverage.standings'))
                   .withColumn('coverage_top_assists', col('coverage.top_assists'))
                   .withColumn('coverage_top_cards', col('coverage.top_cards'))
                   .withColumn('coverage_top_scorers', col('coverage.top_scorers'))
                   .withColumn('fixtures', col('coverage.fixtures'))
                   .drop('coverage')
                   .withColumn('fixture_events', col('fixtures.events'))
                   .withColumn('fixture_lineups', col('fixtures.lineups'))
                   .withColumn('fixture_statistics_fixtures', col('fixtures.statistics_fixtures'))
                   .withColumn('fixture_statistics_players', col('fixtures.statistics_players'))
                   .drop('fixtures')
                   )

    def generate_leagues(self):
        self.df = (self.df
                   .withColumn('league_id', col('league.id'))
                   .withColumn('league_logo', col('league.logo'))
                   .withColumn('league_name', lower(col('league.name')))
                   .withColumn('league_type', lower(col('league.type')))
                   .drop('league'))

        window_spec = Window.partitionBy('league_id').orderBy(length('league_id'))
        leagues = self.df.select('league_id', 'country_code', 'league_name', 'league_type', 'league_logo').distinct()

        leagues = leagues.withColumn("row_num", row_number().over(window_spec))
        self.leagues = leagues.filter(col('row_num') == 1).drop("row_num")

        self.df = self.df.drop('league_name', 'league_logo', 'league_type', 'country_code')

        return self.leagues


    def generate_countries(self):
        self.df = self.df.withColumn('country_code',
                                     when(lower(col('country.name')) == 'world', "wrd")
                                     .otherwise(lower(col('country.code')))) \
            .withColumn('country_name',
                        lower(col('country.name'))) \
            .withColumn('country_flag',
                        lower(col('country.flag'))) \
            .drop('country')

        null_codes = self.df.filter(col('country_code').isNull()).select('country_name').distinct()
        if null_codes.count() > 0:
            logging.warning(f'There are null country codes (apart from World) {null_codes.collect()}, setting them to their name, if that is also null, they will be removed')
            self.df = self.df.withColumn('country_code',
                                         when(col('country_code').isNull(), col('country_name')).otherwise(col('country_code')))
            self.df = self.df.dropna(subset=['country_code'])


        countries = self.df.select('country_code', 'country_name', 'country_flag').distinct()
        duplicate_country_codes = countries.groupBy('country_code').count().filter(col('count') > 1).select('country_code').collect()
        repeated_countries = [row['country_code'] for row in duplicate_country_codes]
        if len(repeated_countries) > 0:
            logging.warning("Some countries have the same code: {}\n Changing code, but probably a manual check could be needed".format(repeated_countries))

            self.df = self.df.withColumn('country_code',
                                         when(col('country_code').isin(repeated_countries),
                                              concat(col('country_code'), lit("-"), col('country_name'))
                                              ).otherwise(col('country_code')))


        self.countries = self.df.select('country_code', 'country_name', 'country_flag').distinct()
        self.df = self.df.drop('country_flag', 'country_name')

        return self.countries


    def merge_with_trusted(self, bucket_path, table_path, key_cols = None):
        basic_merge_with_trusted(self.leagues, self.spark, bucket_path, os.path.join(table_path, 'leagues'), ['league_id'], self.is_gcs_enabled)
        basic_merge_with_trusted(self.countries, self.spark, bucket_path, os.path.join(table_path, 'countries'), ['country_code'], self.is_gcs_enabled)


class TMDBProcessor(Processer):
    def __init__(self, spark, df, is_gcs_enabled = False):
        super().__init__(spark, df, is_gcs_enabled)
        self.movie_genre_df=  None
        self.genre_df = None

    def set_genre_df(self,df):
        self.genre_df = df

    def set_movie_genre_df(self,df):
        self.movie_genre_df = df

    def ensure_schema_genres(self):
        self.genre_df = self.genre_df.toDF(*[c.lower() for c in self.genre_df.columns])
        self.genre_df= self.genre_df.withColumnRenamed("genreid","genre_id")
        self.genre_df = self.genre_df.select(["genre_id","genre"])

    def ensure_schema_movie_genres(self):
        self.movie_genre_df = self.movie_genre_df.toDF(*[c.lower() for c in self.movie_genre_df.columns])
        rename_map = {
            "filmid": "film_id",
            "genreid": "genre_id"
        }
        for old, new in rename_map.items():
            if old in self.movie_genre_df.columns:
                self.movie_genre_df = self.movie_genre_df.withColumnRenamed(old, new)
        if "ingestion_time" not in self.movie_genre_df.columns:
            self.movie_genre_df = self.movie_genre_df.withColumn("ingestion_time", current_timestamp())
        self.movie_genre_df = self.movie_genre_df.select(["film_id","genre_id", "ingestion_time"])

    def ensure_schema(self):
        cols= ["film_id","title","original_title","original_language","overview","release_date","revenue","budget","runtime","adult","popularity","vote_average","vote_count","ingestion_time"]

        self.df = self.df.toDF(*[c.lower() for c in self.df.columns])
        rename_map = {
            "filmid": "film_id",
            "collectionid": "collection_id",
            "status_":"status"
        }

        for old, new in rename_map.items():
            if old in self.df.columns:
                self.df = self.df.withColumnRenamed(old, new)
        if "film_id" not in self.df.columns:
            self.df = self.df.withColumnRenamed("id", "film_id")
        for col in cols:
            if col not in self.df.columns:
                if col=="ingestion_time":
                    self.df = self.df.withColumn("ingestion_time", current_timestamp())
                else:
                    self.df = self.df.withColumn(col, lit(0))

        if "genre_ids" in self.df.columns:
            self.movie_genre_df= self.df.select("film_id", "genre_ids", "ingestion_time").withColumn("genre_id", F.explode("genre_ids")).select("film_id", "genre_id","ingestion_time")
        self.df = self.df.select(cols)

    def static_dump(self, path):
        self.df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path+"/movie")
        self.movie_genre_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path+"/movie_genre")
        self.genre_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path+"/genre")

    def combine_dfs(self, processor):
        self.df = self.df.unionByName(processor.df)
        self.movie_genre_df=self.movie_genre_df.unionByName(processor.movie_genre_df)
        
    def type_dump(self, path, name):
        name=name.split('\\')[1]
        df=self.df.select("film_id")
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path+r"/"+name)     

    def merge_with_trusted(self, bucket_path):
        basic_merge_with_trusted(self.df, self.spark, bucket_path, 'movie', ['film_id'], self.is_gcs_enabled)
        path = os.path.join(bucket_path, "movie_genre")
    
        if path_exists(bucket_path, "movie_genre", self.is_gcs_enabled):
            delta_table = DeltaTable.forPath(self.spark, path)

            # Step 1: Delete all existing rows that match any of our new film_ids
            existing_ids = delta_table.toDF().select("film_id").distinct()
            new_ids = self.movie_genre_df.select("film_id").distinct()
            
            ids_to_delete = existing_ids.join(new_ids, "film_id", "inner")
        
            if not ids_to_delete.rdd.isEmpty():
                existing_data = delta_table.toDF()
                data_to_keep = existing_data.join(ids_to_delete, "film_id", "left_anti")
                
                # Combine with new data and overwrite
                combined = data_to_keep.union(self.movie_genre_df)
                combined.write.format("delta").mode("overwrite").save(path)

        
            else:
                # Initial load if table doesn't exist
                self.movie_genre_df.write.format("delta").mode("append").save(path)
