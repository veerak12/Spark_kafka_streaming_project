from kafka import KafkaConsumer
from json import loads
from datetime import datetime
from configparser import ConfigParser

KAFKA_CONSUMER_GROUP_NAME_CONS = "test-consumer-group"

def load_kafka_config(config_file):
    config_obj = ConfigParser()
    try:
        config_obj.read(config_file)
        kafka_host_name = config_obj.get('kafka', 'host')
        kafka_port_no = config_obj.get('kafka', 'port_no')
        kafka_topic_name = config_obj.get('kafka', 'output_topic_name')
        return kafka_host_name, kafka_port_no, kafka_topic_name
    except Exception as ex:
        print("Failed to load Kafka Configuration")
        print(ex)

def create_kafka_consumer(bootstrap_servers, topic_name):
    return KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=KAFKA_CONSUMER_GROUP_NAME_CONS,
        value_deserializer=lambda x: x.decode('utf-8')
    )

def consume_messages(consumer):
    try:
        for message in consumer:
            print(type(message))
            print("Key: ", message.key)
            message = message.value
            print("Message received: ", message)
    except Exception as ex:
        print("Failed to read Kafka message.")
        print(ex)

def main():
    try:
        config_file = "kafka_host.conf"
        kafka_host_name, kafka_port_no, kafka_topic_name = load_kafka_config(config_file)
        bootstrap_servers = f"{kafka_host_name}:{kafka_port_no}"

        print("Kafka Consumer Application Started ... ")
        consumer = create_kafka_consumer(bootstrap_servers, kafka_topic_name)
        consume_messages(consumer)

    except Exception as ex:
        print("Exception in the main block.")
        print(ex)

if __name__ == "__main__":
    main()
