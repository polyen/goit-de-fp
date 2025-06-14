from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
import json
from dotenv import load_dotenv
import os

from kafka.admin import NewTopic

load_dotenv()

kafka_config = {
    "bootstrap_servers": [os.getenv("KAFKA_SERVER")],
    "username": os.getenv("KAFKA_USER"),
    "password": os.getenv("KAFKA_PASSWORD"),
    "security_protocol": 'SASL_PLAINTEXT',
    "sasl_mechanism": 'PLAIN'
}

topic_results_name = 'athlete_event_results'
topic_output_name = 'v_athlete_event_results_output'


def create_admin_client():
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password']
    )
    return admin_client


def create_topic(topic_name):
    new_topic = NewTopic(name=topic_name, num_partitions=2, replication_factor=1)
    admin_client = create_admin_client()
    existing_topics = admin_client.list_topics()

    if topic_name not in existing_topics:
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    else:
        print(f"Topic '{topic_name}' already exists.")
