from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, lower, regexp_replace, struct, to_timestamp, max as spark_max, lit, coalesce, concat, length, row_number, explode,  asc, desc as spark_desc, current_timestamp

def merge_elements(df, key_cols, order_cols=None, desc=False):
    if order_cols is None:
        order_cols = key_cols

    if desc:
        window = Window.partitionBy(key_cols).orderBy([spark_desc(col) for col in order_cols])
    else:
        window = Window.partitionBy(key_cols).orderBy(order_cols)


    df_columns = [column for column in df.columns if column not in key_cols]

    df = (df
          .withColumn("row_num", F.row_number().over(window))
          .groupBy(key_cols)
          .agg(*[
        F.first(F.col(column), ignorenulls=True).alias(column)
        for column in df_columns
    ]))

    return df
