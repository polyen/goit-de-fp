
from pyspark.sql import SparkSession


def get_spark_session():
    spark = SparkSession.builder \
        .appName("FP_P_2") \
        .getOrCreate()

    return spark
