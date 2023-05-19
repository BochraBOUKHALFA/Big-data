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
    """
    Cette méthode permet de générer un DataFrame Pandas pour alimenter vos data
    """
    df = pd.DataFrame(columns=col)
    add_data(df)
    return df


def add_data(df: pd.DataFrame):
    i = 0

    while i < 600:
        timestamp = datetime.datetime.now()
        name = random.choice(["Fluffy", "Whiskers", "Mittens", "Snowball", "Yuki"])

        # Introduce false data
        if random.random() < 0.02:  # 20% chance of having null values
            age = np.nan
            breed = None
        else:
            age = random.randint(-10, 10)  # Allow negative ages
            breed = random.choice(["Siamese", "Persian", "Maine Coon", "Bengal"])

        df.loc[i] = [timestamp,name, age,breed]
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

    bucket_name = "catbucket"
    file_name = "cat.csv"

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
    columns = ['Timestamp', 'Name', 'Age', 'Breed']
    df = generate_dataFrame(columns)
    write_data_minio(df)