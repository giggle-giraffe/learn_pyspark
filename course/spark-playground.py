from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("SparkPlayground") \
    .config("spark.jars", "../jars/postgresql-42.2.19.jar") \
    .getOrCreate()


def demo_app():
    df = spark.read.json("../data/cars")
    df.show()

def first_exercise():
    df = spark.read.json("../data/cars")
    df_test = df.select(["Name", "Weight_in_lbs"]).withColumn("Weight_in_kg", F.col("Weight_in_lbs") / 2.2)
    df_test.show()

def demo_formats():
    df = spark.read.json("../data/movies")
    df = df.where("IMDB_Rating > 6 and Major_Genre = 'Comedy'")
    df.write \
        .option("header", "true") \
        .option("sep", "\t") \
        .csv("../data/movies_csv")


def demo_aggregations():
    original_df = spark.read.json("../data/movies")

    # select(sum...), countDistinct return a DF with a single value
    df = original_df.select(F.sum(original_df.Worldwide_Gross).alias("worldwide_gross"))
    df.show()

    df1 = original_df.select(F.countDistinct("Director").alias("distinct_director_count"))
    df1.show()

    df2 = original_df.select(F.mean(original_df.US_Gross).alias("mean_us_gross"))
    df2.show()

    # grouped DF -> compute stats per unique group
    df3 = original_df.groupBy("Director") \
        .agg(F.avg("IMDB_Rating").alias("avg_rating"), F.avg("US_Gross").alias("avg_us_gross"))
    df3.show()


# joins
# read all the tables from the database
def read_table(table_name):
    # -- reading the data
    return spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5432/rtjvm") \
        .option("user", "docker") \
        .option("password", "docker") \
        .option("dbtable", "public." + table_name) \
        .load()


def demo_joins():
    departments_df = read_table("departments")
    dept_emp_df = read_table("dept_emp")
    dept_manager_df = read_table("dept_manager")
    employees_df = read_table("employees")
    salaries_df = read_table("salaries")
    titles_df = read_table("titles")

    """
    Exercises
        - show all employees and their max salary
        - show all employees who were never managers
    """
    df_max_salary = dept_emp_df[F.col("emp_no"), F.col("dept_no")].join(salaries_df[F.col("emp_no"), F.col("salary")], on="emp_no", how="left").groupBy("emp_no").agg(F.max("salary").alias("MaxSalary"))

    df_max_salary.show()

    dept_manager_df.show()

    df_no_manager = dept_emp_df[F.col("emp_no"), F.col("dept_no")].distinct().join(dept_manager_df, on=["emp_no", "dept_no"], how="left").filter(F.isnull(F.col("from_date")))
    df_no_manager = df_no_manager.distinct()

    df_no_manager.show()
    print(dept_emp_df.agg(F.countDistinct("emp_no")).collect()[0][0])
    print(df_no_manager.agg(F.countDistinct("emp_no")).collect()[0][0])

    managers_df = employees_df.join(titles_df.filter(F.col("title") == "Manager"), on="emp_no", how="outer")
    managers_df.show()


if __name__ == "__main__":
    # first_exercise()
    # demo_formats()
    # demo_aggregations()
    demo_joins()
