import os
from xmlrpc.client import ResponseError

import pandas as pd
import numpy as np
import datetime
from kafka import *
from minio import Minio
from minio.error import S3Error
import datetime
import json
import datetime
import random

"""
Mini-Projet : Traitement de l'Intelligence Artificielle
Contexte : Allier les concepts entre l'IA, le Big Data et IoT

Squelette pour simuler un capteur qui est temporairement stocké sous la forme de Pandas
"""

"""
    Dans ce fichier capteur.py, vous devez compléter les méthodes pour générer les données brutes vers Pandas 
    et par la suite, les envoyer vers Kafka grace au fichier consummer.py.
    ---
    
    n'oubliez pas de creér de la donner avec des valeur nulles, de fausse valeur ( par exemple negatives pour les valeur
    qui initialement doivent etre entre 0 et 100 ), et de la valeur faussement typer ( je veux par exemple une valeur
    string qui doit a la base être en int)

"""

def generate_dataFrame(col):
    df = pd.DataFrame(columns=col)
    add_data(df)
    return df


def add_data(df: pd.DataFrame):
    i = 0

    while i < 1000:
        timestamp = datetime.datetime.now()
        compteur_id = random.randint(1, 1000)
        voltage = round(random.uniform(220.0, 240.0),2) # Random voltage between 220V and 240V
        consumption_KW = round(random.uniform(1.0, 1000.0),2)
        price = round(random.uniform(1.0, 1000.0),2)
        id_Machine = random.randint(1, 1000)
        id_consumer = random.randint(1, 1000)
        Nbr_Person = random.randint(1,1000)
        Nbr_machine= random.randint(1,500)
        # current = random.uniform(0.0, 10.0)

        # Introduce false data
        if random.random() < 0.2:  # 20% chance of having null values
            power_factor = np.nan
            current = None
        else:
            current = random.randint(-10.0, 10.0) #Random current between 0A and 10A (added (-10 ,10) range to insert a negative number)
            power_factor = round(random.uniform(0.8, 1.0), 2)


        df.loc[i] = [timestamp,voltage,compteur_id, current,power_factor ,consumption_KW , price , id_Machine ,id_consumer,Nbr_Person,Nbr_machine]
        i += 1

    return df

def write_data_minio(df: pd.DataFrame):
    # Configure Minio client
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )

    bucket_name = "compteurbucket"
    file_name = "compteurs.csv"

    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")

        temp_file = "temp.csv"
        df.to_csv(temp_file, index=False)


        client.fput_object(bucket_name, file_name, temp_file)
        os.remove(temp_file)

        print("DataFrame successfully written to Minio.")
    except ResponseError as err:
        print(f"Error occurred while writing DataFrame to Minio: {err}")

if __name__ == "__main__":
    columns = ['Timestamp', 'voltage', 'compteur_id', 'current', 'power_factor', 'consumption_KW', 'price', 'id_Machine', 'id_consumer', 'Nbr_Person', 'Nbr_machine']
    df = generate_dataFrame(columns)
    write_data_minio(df)

