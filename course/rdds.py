from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("RDD Demo") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "100s") \
    .config("spark.python.worker.reuse", "true") \
    .getOrCreate()

# RDDs need Spark Context
sc = spark.sparkContext

def app():
    movies_df = spark.read.json("../data/movies")
    movies_rdd = movies_df.rdd
    grouped_by_genre = movies_rdd.groupBy(lambda row: row.Major_Genre)


if __name__ == "__main__":
    app()
    # chunks = partitions
    numbers = sc.parallelize(range(1000000))

    # FP primitives
    tenx_numbers = numbers.map(lambda x: x * 10)  # new RDD with new numbers
    even_numbers = numbers.filter(lambda x: x % 2 == 0)
    grouped_numbers = numbers.groupBy(lambda x: x % 3)

    print(tenx_numbers.collect()[:10])  # action
    print(even_numbers.collect()[:10])
    # print(grouped_numbers.collect()[:10])
