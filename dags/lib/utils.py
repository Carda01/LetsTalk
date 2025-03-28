import pyspark, os, logging
from delta import configure_spark_with_delta_pip
from airflow.models import Variable

def get_spark_and_path():
    is_gcs_enabled = Variable.get("is_gcs_enabled", "False")
    if is_gcs_enabled == "True":
        spark = create_spark_local_session()
        delta_table_base_path = "gs://letstalk_landing_zone_bdma"
    else:
        spark = create_spark_gcs_session()
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


def ingest_data(**kwargs):
    temp_file_paths = kwargs['ti'].xcom_pull(task_ids=f'fetch_from_news_api')
    spark = create_spark_session()

    for category, tmp_json_path in temp_file_paths.items():
        df = spark.read.json(tmp_json_path)

        delta_table_path = f"/data/delta_news/{category}"
        os.makedirs(os.path.dirname(delta_table_path), exist_ok=True)

        df.write.mode("append").format("delta").save(delta_table_path)
        logging.info(f"Writing to Delta Lake at {delta_table_path}")

        os.unlink(tmp_json_path)

    spark.stop()
    logging.info("Spark session stopped successfully")
