import json
from kafka import KafkaProducer
import datetime
import numpy as np
import time
import random


def add_data():
    while True:
        timestamp = datetime.datetime.now().isoformat()  # Convert datetime to string
        compteur_id = random.randint(1, 1000)
        voltage = round(random.uniform(220.0, 240.0), 2)
        consumption_KW = round(random.uniform(1.0, 1000.0), 2)
        price = round(random.uniform(1.0, 1000.0), 2)
        id_Machine = random.randint(1, 1000)
        id_consumer = random.randint(1, 1000)
        Nbr_Person = random.randint(1, 1000)
        Nbr_machine = random.randint(1, 500)

        if random.random() < 0.2:
            power_factor = np.nan
            current = None
        else:
            current = random.uniform(0.0, 10.0)
            power_factor = round(random.uniform(0.8, 1.0), 2)

        data = {
            "compteur_id": compteur_id,
            "voltage": voltage,
            "current": current,
            "power_factor": power_factor,
            "timestamp": timestamp,
            'consumption_KW': consumption_KW,
            'price': price,
            'id_Machine': id_Machine,
            'id_consumer': id_consumer,
            'Nbr_Person': Nbr_Person,
            'Nbr_machine': Nbr_machine
        }

        return data


def kafka_producer(topic):
    producer = KafkaProducer(bootstrap_servers=['kafka:2909'],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    while True:
        data = add_data()

        producer.send(topic, value=data)
        producer.flush()
        time.sleep(1)


if __name__ == "__main__":
    topic = "send_compteur_data"
    kafka_producer(topic)
