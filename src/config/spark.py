from pyspark.sql import SparkSession


def get_spark_session(app_name: str, driver_path: str) -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", driver_path) \
        .getOrCreate()
    return spark
