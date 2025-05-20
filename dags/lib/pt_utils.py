import pyspark, os, logging
from delta import configure_spark_with_delta_pip
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.window import Window
from pyspark.sql.functions import desc as spark_desc

def get_landing_path(path):
    return os.path.join(path, 'letstalk_landing_zone_bdma')

def get_trusted_path(path):
    return os.path.join(path, 'letstalk_trusted_zone_bdma')

def get_explotation_path(path):
    return os.path.join(path, 'letstalk_explotation_zone_bdma')

def get_spark_and_path(is_gcs_enabled):
    logging.info(is_gcs_enabled)
    if is_gcs_enabled == "True":
        logging.info(is_gcs_enabled)
        spark = create_spark_gcs_session()
        delta_table_base_path = "gs://"
    else:
        logging.info(is_gcs_enabled)
        spark = create_spark_local_session()
        delta_table_base_path = "/data"

    logging.info(is_gcs_enabled)
    return spark, delta_table_base_path


def create_spark_gcs_session():
    conf = (
        pyspark.conf.SparkConf()
        .setAppName("LetsTalk")
        .set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "gcs/gcs.json")
        .set("spark.sql.shuffle.partitions", "4")
        .set("spark.driver.memory", "2g") \
        .set("spark.executor.memory", "2g") \
        .set("spark.jars", "gcs/gcs-connector-hadoop.jar")
        .setMaster(
            "local[*]"
        )
    )

    builder = pyspark.sql.SparkSession.builder.appName("LetsTalk").config(conf=conf)
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def create_spark_local_session():
    builder = pyspark.sql.SparkSession.builder.appName("LetsTalk") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def get_null_percentage(df):
    total_rows = df.count()
    null_condition = reduce(lambda acc, c: acc | F.col(c).isNull(), df.columns, F.lit(False))
    rows_with_null = df.filter(null_condition).count()
    return (rows_with_null / total_rows) * 100


def merge_elements(df, key_cols, order_cols=None, desc=False):
    if order_cols is None:
        order_cols = key_cols

    if desc:
        window = Window.partitionBy(key_cols).orderBy([spark_desc(col) for col in order_cols])
    else:
        window = Window.partitionBy(key_cols).orderBy(order_cols)


    df_columns = [column for column in df.columns if column not in key_cols]

    res_df = (df
          .withColumn("row_num", F.row_number().over(window))
          .groupBy(key_cols)
          .agg(*[
        F.first(F.col(column), ignorenulls=True).alias(column)
        for column in df_columns
    ]))

    return res_df
