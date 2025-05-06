import logging
from abc import ABC, abstractmethod

from delta import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, lower, regexp_replace, struct, to_timestamp, max as spark_max, lit, coalesce, concat, length, row_number, explode
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType


class Processer(ABC):

    def __init__(self, spark, df):
        logging.basicConfig(level=logging.INFO)
        self.spark = spark
        self.df = df


    @abstractmethod
    def ensure_schema(self):
        pass


    def remove_clear_duplicates(self):
        initial_count = self.df.count()
        self.df = self.df.dropDuplicates()
        final_count = self.df.count()
        logging.info(f"Removed {initial_count - final_count} simple duplicate(s)")


    def remove_hidden_duplicates(self, key_cols, order_cols):
        window = Window.partitionBy(key_cols).orderBy(order_cols)
        initial_count = self.df.count()
        df_columns = [column for column in self.df.columns if column not in key_cols]

        self.df = (self.df
                     .withColumn("row_num", F.row_number().over(window))
                     .groupBy(key_cols)
                     .agg(*[
            F.first(F.col(column), ignorenulls=True).alias(column)
            for column in df_columns
        ]))
        final_count = self.df.count()
        logging.info(f"Removed {initial_count - final_count} hidden duplicate(s)")


    def normalize_text(self, columns):
        for column in columns:
            self.df = self.df.withColumn(column, lower(col(column)))
            self.df = self.df.withColumn(column, regexp_replace(col(column), r"http\S+|www\.\S+", " "))
            self.df = self.df.withColumn(column, regexp_replace(col(column), r"[^a-zA-Z\s]", " "))


    def order_by(self, column, ascending=True):
        self.df = self.df.orderBy(column, ascending=ascending)


    def merge_with_trusted(self, path, key_cols):
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




class NewsProcessor(Processer):
    def __init__(self, spark, df):
        super().__init__(spark, df)


    def ensure_schema(self):
        self.df = (self.df.withColumn("publishedAt",
                        to_timestamp(col("publishedAt"), "yyyy-MM-dd'T'HH:mm:ssX")))

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



class SportsProcessor(Processer):
    def __init__(self, spark, df):
        super().__init__(spark, df)


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
        leagues = leagues.filter(col('row_num') == 1).drop("row_num")

        self.df = self.df.drop('league_name', 'league_logo', 'league_type', 'country_code')

        return leagues


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


        countries = self.df.select('country_code', 'country_name', 'country_flag').distinct()
        self.df = self.df.drop('country_flag', 'country_name')

        return countries
