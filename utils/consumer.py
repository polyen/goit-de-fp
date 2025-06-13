import os

from utils.kafka_helpers import topic_output_name, kafka_config
from utils.spark_helpers import get_spark_session


def run_consumer():
    spark = get_spark_session()
    kafka_stream_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_SERVER")) \
        .option("subscribe", topic_output_name) \
        .option("kafka.security.protocol", kafka_config["security_protocol"]) \
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
        .option("kafka.sasl.jaas.config",
                f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" '
                f'password="{kafka_config["password"]}";') \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "50") \
        .load()

    # show results in console
    kafka_stream_df = kafka_stream_df.selectExpr("CAST(value AS STRING)")
    kafka_stream_df = kafka_stream_df.selectExpr("value")
    query = kafka_stream_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    query.awaitTermination()


if __name__ == '__main__':
    run_consumer()
