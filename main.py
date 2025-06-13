import os

from pyspark.sql.functions import col, from_json, expr, current_timestamp, avg, round
from pyspark.sql.types import DoubleType, StructType, StructField, StringType

from utils.kafka_helpers import topic_results_name, kafka_config, create_topic, topic_output_name
from utils.foreach_batch_function import foreach_batch_function
from utils.spark_helpers import get_spark_session
from utils.mysql_helpers import read_sql_table


def stream_results_to_kafka():
    # 3. Зчитати дані з mysql таблиці athlete_event_results і записати в кафка топік athlete_event_results
    spark = get_spark_session()
    df_event_results = read_sql_table(spark, "athlete_event_results")

    df_event_results.selectExpr("CAST(athlete_id AS STRING) as key", "to_json(struct(*)) AS value") \
        .write.format("kafka") \
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_SERVER")) \
        .option("topic", topic_results_name) \
        .option("kafka.security.protocol", kafka_config["security_protocol"]) \
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
        .option("kafka.sasl.jaas.config",
                f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" '
                f'password="{kafka_config["password"]}";') \
        .save()

    print('Data streamed to Kafka topic:', topic_results_name)


def get_athlete_bio(spark):
    # 1. Зчитати дані фізичних показників атлетів за допомогою Spark з MySQL таблиці olympic_dataset.athlete_bio

    df_athlete_bio = read_sql_table(spark, "athlete_bio")

    columns_to_check = ['height', 'weight']

    # 2. Відфільтрувати дані, де показники зросту та ваги є порожніми або не є числами.
    for col_name in columns_to_check:
        df_athlete_bio = df_athlete_bio.filter(
            expr(f"try_cast({col_name} as double) is not null")
        )

    df_athlete_bio = df_athlete_bio.withColumn("height", col("height").cast(DoubleType())) \
        .withColumn("weight", col("weight").cast(DoubleType())) \
        .withColumn("athlete_id", col("athlete_id").cast(StringType()))

    return df_athlete_bio


def run_stream():
    spark = get_spark_session()

    df_athlete_bio = get_athlete_bio(spark)

    schema = StructType([
        StructField("edition", StringType(), True),
        StructField("edition_id", StringType(), True),
        StructField("country_noc", StringType(), True),
        StructField("sport", StringType(), True),
        StructField("event", StringType(), True),
        StructField("result_id", StringType(), True),
        StructField("athlete", StringType(), True),
        StructField("athlete_id", StringType(), True),
        StructField("pos", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("isTeamSport", StringType(), True)
    ])

    # 3a Зчитати дані з результатами змагань з Kafka-топіку athlete_event_results
    kafka_stream_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_SERVER")) \
        .option("subscribe", topic_results_name) \
        .option("kafka.security.protocol", kafka_config["security_protocol"]) \
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
        .option("kafka.sasl.jaas.config",
                f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" '
                f'password="{kafka_config["password"]}";') \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "50") \
        .load()

    kafka_stream_df = kafka_stream_df.selectExpr("CAST(value AS STRING)")
    kafka_stream_df = kafka_stream_df.select(from_json(col("value").cast("string"), schema).alias("data")).select(
        "data.*")

    kafka_stream_df = kafka_stream_df.withColumn("athlete_id", col("athlete_id").cast(StringType())) \
        .withColumn("edition", col("edition").cast(StringType())) \
        .withColumn("edition_id", col("edition_id").cast(StringType())) \
        .withColumn("country_noc", col("country_noc").cast(StringType())) \
        .withColumn("sport", col("sport").cast(StringType())) \
        .withColumn("event", col("event").cast(StringType())) \
        .withColumn("result_id", col("result_id").cast(StringType())) \
        .withColumn("pos", col("pos").cast(StringType())) \
        .withColumn("medal", col("medal").cast(StringType())) \
        .withColumn("isTeamSport", col("isTeamSport").cast(StringType()))

    kafka_stream_df = kafka_stream_df.select(
        col("edition"),
        col("edition_id"),
        col("country_noc"),
        col("sport"),
        col("event"),
        col("result_id"),
        col("athlete_id"),
        col("pos"),
        col("medal"),
        col("isTeamSport")
    )

    # 4. Об’єднати дані з результатами змагань з Kafka-топіку з біологічними даними з MySQL таблиці за допомогою ключа athlete_id.

    joined_df = kafka_stream_df.alias("kafka").join(
        df_athlete_bio.alias("bio"),
        col("kafka.athlete_id") == col("bio.athlete_id"),
        "inner"
    ).select(
        col("kafka.sport"),
        col("kafka.medal"),
        col("bio.sex"),
        col("kafka.country_noc").alias("country_noc"),
        col("bio.height"),
        col("bio.weight")
    )

    # 5. Знайти середній зріст і вагу атлетів індивідуально для кожного виду спорту, типу медалі або її відсутності, статі, країни
    aggregated_df = joined_df.groupBy(
        "sport", "medal", "sex", "country_noc"
    ).agg(
        round(avg("height"), 2).alias("avg_height"),
        round(avg("weight"), 2).alias("avg_weight"),
        current_timestamp().alias("timestamp")
    )

    # 6. Зробіть стрим даних (за допомогою функції forEachBatch) у:

    aggregated_df \
        .writeStream \
        .foreachBatch(foreach_batch_function) \
        .outputMode("update") \
        .start() \
        .awaitTermination()


def main():
    create_topic(topic_output_name)
    stream_results_to_kafka()
    run_stream()


if __name__ == "__main__":
    main()
