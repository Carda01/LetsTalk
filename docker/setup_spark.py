import pyspark
from delta import configure_spark_with_delta_pip

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

spark = create_spark_gcs_session()
spark.stop()
