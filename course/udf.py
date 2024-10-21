from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("UDFs") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "100s") \
    .config("spark.python.worker.reuse", "true") \
    .getOrCreate()

# UDF = user-defined functions
# plymouth satellite -> Plymouth Satellite
def convert_case(name):
    words = name.split(" ")
    return " ".join([word[0].upper() + word[1:] for word in words if len(word) > 0])

def demo_udf():
    convert_case_udf = udf(convert_case, StringType()) # register a UDF for a particular column type
    # cars_formatted_df = spark.read.json("../data/cars").select(convert_case_udf(col("Name")).alias("Name_Capitalized"))
    cars_formatted_df = spark.read.json("../data/cars").select(upper(col("Name"))).alias("Name_Capitalized")
    cars_formatted_df.show()

if __name__ == '__main__':
    demo_udf()
