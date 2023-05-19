import json

from kafka import KafkaProducer
import datetime
import numpy as np
import time
import random


def add_data():

    while True:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        name = random.choice(["Fluffy", "Whiskers", "Mittens", "Snowball", "Yuki"])

        # Introduce false data
        if random.random() < 0.02:  # 20% chance of having null values
            age = np.nan
            breed = None
        else:
            age = random.randint(-10, 10)  # Allow negative ages
            breed = random.choice(["Siamese", "Persian", "Maine Coon", "Bengal"])

        data = [timestamp,name, age,breed]

        return data

def kafka_producer(topic):

    producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'])

    while True:
        data = add_data()

        # Serialize data to JSON string
        json_data = json.dumps(data)

        # Convert JSON string to bytes
        value_bytes = json_data.encode('utf-8')

        # Send data to Kafka topic
        producer.send(topic, value=value_bytes)

        # Flush producer to ensure data is sent
        producer.flush()

        # Sleep for 1 second
        time.sleep(1)

if __name__ == "__main__":
    # Kafka configuration
    topic = "sending_cats"

    kafka_producer(topic)
