from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import pandas as pd
import numpy as np
from time import time


spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Arrow Demo") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "100s") \
    .config("spark.python.worker.reuse", "true") \
    .getOrCreate()


def multiply_by_two(x):
    return x * 2


def multiply_by_two_vectorized(column):
    return column * 2


def demo_arrow():
    num_rows = 1000000
    df = spark.createDataFrame(
        pd.DataFrame({
            "id": np.arange(num_rows),
            "value": np.random.randn(num_rows)
        })
    )

    regular_udf = F.udf(multiply_by_two, DoubleType())

    start_time = time()
    df_with_regular_udf = df.withColumn("result_regular", regular_udf(F.col("value")))
    df_with_regular_udf.show()
    print(f"Regular UDF time {time() - start_time} seconds")

    spark.conf.set("spark.sql.execution.arraw.pyspark.enabled", "true")

    vectorized_udf = F.pandas_udf(multiply_by_two_vectorized, DoubleType())

    start_time = time()
    df_with_vectorized_udf = df.withColumn("result_vectorized", vectorized_udf(F.col("value")))
    df_with_vectorized_udf.show()
    print(f"Vectorized UDF time {time() - start_time} seconds")


if __name__ == "__main__":
    demo_arrow()
