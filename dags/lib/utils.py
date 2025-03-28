import pyspark, os, logging
from delta import configure_spark_with_delta_pip
from airflow.models import Variable
from pyspark.sql import functions as F
from functools import reduce

def get_spark_and_path():
    is_gcs_enabled = Variable.get("is_gcs_enabled", "False")
    if is_gcs_enabled == "True":
        spark = create_spark_gcs_session()
        delta_table_base_path = "gs://letstalk_landing_zone_bdma"
    else:
        spark = create_spark_local_session()
        delta_table_base_path = "/data"

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
        .set("spark.jars", "gcs/gcs-connector-hadoop3-latest.jar")
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
