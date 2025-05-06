from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql import Row
from datetime import datetime
import os
from delta import *

def get_control_table_schema():
    return StructType([
        StructField("source_table", StringType(), False),
        StructField("last_processed_version", LongType(), False),
        StructField("last_run_ts", TimestampType(), True)
    ])

def create_control_table(path, spark):
    empty_df = spark.createDataFrame([], get_control_table_schema())

    empty_df.write.format("delta").mode("overwrite").save(path)


def does_cdf_make_sense(delta_table, last_version):
    history_df = delta_table.history()
    for row in history_df.collect():
        if '"delta.enableChangeDataFeed":"true"' in row['operationParameters'].get("properties", ""):
            cdf_start_version = row["version"]
            break
    else:
        cdf_start_version = None

    if cdf_start_version is not None and last_version >= cdf_start_version:
        return True

    return False


def is_cdf_enabled(delta_table):
    properties = delta_table.detail().selectExpr("properties['delta.enableChangeDataFeed']").collect()
    return properties[0][0] == "true"


class IncrementalLoader:
    def __init__(self, spark, absolute_base_path, table_name):
        self.spark = spark
        self.base_path = absolute_base_path
        self.control_table_path = os.path.join(self.base_path, "control_table")
        if not os.path.exists(self.control_table_path):
            os.makedirs(self.control_table_path)
            create_control_table(self.control_table_path, spark)
        self.table_name = table_name
        self.landing_path = str(os.path.join(self.base_path, self.table_name))
        self.latest_version = None


    def update_control_table(self):
        new_row = self.spark.createDataFrame([
            Row(source_table=self.table_name,
                last_processed_version=self.latest_version,
                last_run_ts=datetime.now())
        ], schema=get_control_table_schema())

        control_table = DeltaTable.forPath(self.spark, self.control_table_path)


        control_table.alias("target").merge(
            new_row.alias("source"),
            "target.source_table = source.source_table"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


    def enable_cdf(self):
        self.spark.sql(f"""
          ALTER TABLE delta.`{self.landing_path}`
          SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)


    def get_last_processed_version(self):
        df = self.spark.read.format("delta").load(self.control_table_path)
        result = df.filter(df.source_table == self.table_name).collect()
        if result:
            return result[0]['last_processed_version']
        else:
            return 0


    def get_latest_version(self):
        delta_table = DeltaTable.forPath(self.spark, self.landing_path)
        return delta_table.history(1).select("version").collect()[0][0]


    def get_new_data(self):
        last_processed_version = self.get_last_processed_version()
        delta_table = DeltaTable.forPath(self.spark, self.landing_path)

        cdf_enabled = is_cdf_enabled(delta_table)
        latest_version = self.get_latest_version()

        use_cdf = cdf_enabled and does_cdf_make_sense(delta_table, last_processed_version)

        if use_cdf:
            print(f"Using CDF from version {last_processed_version}")
            df = self.spark.read.format("delta") \
                .option("readChangeData", "true") \
                .option("startingVersion", str(last_processed_version)) \
                .option("endingVersion", str(latest_version)) \
                .load(self.landing_path) \
                .filter("_change_type = 'insert'")
            df = df.drop("_change_type", "_commit_version", "_commit_timestamp")
        else:
            print("CDF not available — doing full load")
            if not cdf_enabled:
                self.enable_cdf()
            latest_version = self.get_latest_version()
            df = self.spark.read.format("delta") \
                .option("versionAsOf", latest_version) \
                .load(self.landing_path)

        self.latest_version = latest_version
        return df

