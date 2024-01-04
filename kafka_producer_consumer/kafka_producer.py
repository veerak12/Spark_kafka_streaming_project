from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random
from configparser import ConfigParser

def load_kafka_config(config_file):
    config_obj = ConfigParser()
    try:
        config_obj.read(config_file)
        kafka_host_name = config_obj.get('kafka', 'host')
        kafka_port_no = config_obj.get('kafka', 'port_no')
        kafka_topic_name = config_obj.get('kafka', 'input_topic_name')
        return kafka_host_name, kafka_port_no, kafka_topic_name
    except Exception as ex:
        print("Failed to load Kafka configuration.")
        print(ex)

def create_kafka_producer(bootstrap_servers):
    return KafkaProducer(bootstrap_servers=bootstrap_servers,
                        value_serializer=lambda x: dumps(x).encode('utf-8'))

def generate_random_order(order_id):
    try:
        product_name_list = ["Laptop", "Desktop Computer", "Mobile Phone", "Wrist Band", "Wrist Watch", "LAN Cable",
                             "HDMI Cable", "TV", "TV Stand", "Text Books", "External Hard Drive", "Pen Drive", "Online Course"]

        order_card_type_list = ['Visa', 'Master', 'Maestro', 'American Express', 'Cirrus', 'PayPal']

        country_name_city_name_list = ["Sydney,Australia", "Florida,United States", "New York City,United States",
                                       "Paris,France", "Colombo,Sri Lanka", "Dhaka,Bangladesh", "Islamabad,Pakistan",
                                       "Beijing,China", "Rome,Italy", "Berlin,Germany", "Ottawa,Canada",
                                       "London,United Kingdom", "Jerusalem,Israel", "Bangkok,Thailand",
                                       "Chennai,India", "Bangalore,India", "Mumbai,India", "Pune,India",
                                       "New Delhi,Inida", "Hyderabad,India", "Kolkata,India", "Singapore,Singapore"]

        ecommerce_website_name_list = ["www.datamaking.com", "www.amazon.com", "www.flipkart.com", "www.snapdeal.com", "www.ebay.com"]

        order = {}
        event_datetime = datetime.now()
        order["order_id"] = order_id
        order["order_product_name"] = random.choice(product_name_list)
        order["order_card_type"] = random.choice(order_card_type_list)
        order["order_amount"] = round(random.uniform(5.5, 555.5), 2)
        order["order_datetime"] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")

        country_name_city_name = random.choice(country_name_city_name_list)
        order["order_country_name"] = country_name_city_name.split(",")[1]
        order["order_city_name"] = country_name_city_name.split(",")[0]
        order["order_ecommerce_website_name"] = random.choice(ecommerce_website_name_list)

        return order
    except Exception as ex:
        print("Failed to generate random order.")
        print(ex)

def get_last_order_id():
    try:
        with open('last_order_id.txt', 'r') as file:
            return int(file.read())
    except FileNotFoundError:
        return 0

def save_last_order_id(order_id):
    with open('last_order_id.txt', 'w') as file:
        file.write(str(order_id))

def main():
    try:
        conf_file_name = "kafka_host.conf"
        kafka_host_name, kafka_port_no, kafka_topic_name = load_kafka_config(conf_file_name)
        bootstrap_servers = f"{kafka_host_name}:{kafka_port_no}"
        kafka_producer_obj = create_kafka_producer(bootstrap_servers)

        print("Kafka Producer Application Started ... ")
        order_id = get_last_order_id()+1
        for i in range(100):
            print(f"Preparing message: {order_id}")
            order = generate_random_order(order_id)
            kafka_producer_obj.send(kafka_topic_name, order)
            time.sleep(1)
            save_last_order_id(order_id)
            order_id += 1

        print("Kafka Producer Application Completed.")
    except Exception as ex:
        print("An error occurred in the main function.")
        print(ex)

if __name__ == "__main__":
    main()
