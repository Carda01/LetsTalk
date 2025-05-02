import logging
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, lower, regexp_replace, struct


class Processer:
    def __init__(self, spark, df):
        logging.basicConfig(level=logging.INFO)
        self.spark = spark
        self.df = df


    def remove_clear_duplicates(self):
        initial_count = self.df.count()
        self.df = self.df.dropDuplicates()
        final_count = self.df.count()
        print(f"Removed {initial_count - final_count} simple duplicate(s)")


    def remove_hidden_duplicates(self, key_cols):
        window = Window.partitionBy(key_cols).orderBy(F.col("publishedAt").desc())
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
        print(f"Removed {initial_count - final_count} hidden duplicate(s)")



class NewsProcessor(Processer):
    def __init__(self, spark, df):
        super().__init__(spark, df)


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
