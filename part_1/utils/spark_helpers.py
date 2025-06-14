from pyspark.sql import SparkSession


def get_spark_session():
    spark = SparkSession.builder \
        .config("spark.jars.packages",
                "mysql:mysql-connector-java:8.0.32,org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
        .appName("FP_P_1") \
        .getOrCreate()

    return spark
