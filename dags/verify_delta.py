from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("DeltaReader") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

base_path = "/home/nima/data"  
delta_path = f"{base_path}/delta_news"

print("Available categories:")
for category in os.listdir(delta_path):
    category_path = f"{delta_path}/{category}"
    if os.path.isdir(category_path):
        df = spark.read.format("delta").load(category_path)
        count = df.count()
        print(f"  - {category}: {count} articles")
        if count > 0:
            print("Sample data:")
            df.select("title", "description", "publishedAt").show(2, truncate=50)

spark.stop()