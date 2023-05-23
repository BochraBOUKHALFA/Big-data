import json
from kafka import KafkaProducer
import datetime
import numpy as np
import time
import random


def add_data():
    while True:
        timestamp = datetime.datetime.now().isoformat()
        compteur_id = random.randint(1, 1000)
        voltage = random.uniform(220.0, 240.0)

        if random.random() < 0.02:
            power_factor = np.nan
            current = None
        else:
            current = random.randint(-10, 10)
            power_factor = round(random.uniform(0.8, 1.0), 2)

        data = {
            "compteur_id": compteur_id,
            "voltage": voltage,
            "current": current,
            "power_factor": power_factor,
            "timestamp": timestamp
        }

        return data


def kafka_producer(topic):
    producer = KafkaProducer(bootstrap_servers=['kafka:2909'],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    while True:
        data = add_data()

        # Send data to Kafka topic
        producer.send(topic, value=data)

        # Flush producer to ensure data is sent
        producer.flush()

        # Sleep for 1 second
        time.sleep(1)


if __name__ == "__main__":
    # Kafka configuration
    topic = "send_compteur_data"
    kafka_producer(topic)
