import os

from part_1.utils.kafka_helpers import topic_output_name, kafka_config
from part_1.utils.mysql_helpers import write_sql_table


def foreach_batch_function(batch_df, batch_id):
    # а) вихідний кафка-топік,
    batch_df.selectExpr("CAST(sport AS STRING) AS key", "to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_SERVER")) \
        .option("topic", topic_output_name) \
        .option("kafka.security.protocol", kafka_config["security_protocol"]) \
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
        .option("kafka.sasl.jaas.config",
                f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" '
                f'password="{kafka_config["password"]}";') \
        .save()

    # b) базу даних.
    write_sql_table(batch_df, "v_athlete_event_results_output")
