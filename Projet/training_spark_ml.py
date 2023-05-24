import io
import logging
import sys
import traceback
from datetime import datetime

import urllib3

from minio import Minio
from pyspark import SparkContext
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import unix_timestamp
import minio


def create_model(iter, features_cols, labelCol):
    """
        Cette methode permet de créer un modèle de regression linéaire dont vous devez inclure trois paramètres
        - maxIter : Le nombre d'iteration maximale
        - featuresCol : Les colonnes que le modèle va utiliser pour prédire le résultat attendu
        - labelCol : Le nom de la colonne dont contient le résultat
        Retourne un modèle de Régression Linéaire
    """

    lr = LinearRegression(maxIter=iter,
                          featuresCol=features_cols,
                          labelCol=labelCol)
    return lr


def set_train_and_validation_ds(data, seed):
    """
    Permet de diviser le "data" en deux sous DataFrame entrainement et validation à hauteur de 70%/30%
    Retourne deux DataFrame Spark
    """
    return data.randomSplit([0.7, 0.3], seed=seed)


def test_model(spark, model):
    """
    Méthode qui permet de tester la performance du model en fonction du jeux de test fournis
    (Optionnel)
    """
    test_data = spark.read.format("libsvm").load("path/to/test/file")
    predictions = model.transform(test_data)
    predictions.show()


def save_model(model):
    """
    Permet de sauvegarder un modèle entrainé.
    """
    # Methode pour sauvegarder le modèle vers un lieu spécifié
    model.save("saved_model/model_test")


def main():
    """
    Constante de reproductibilité sur les fonctions aléatoires lors de la division du DataFrame
    et des étapes de l'entrainement. Ne pas changer.
    """
    seed = 49

    """
    Initialisation d'un environnement Spark
    """
    try:
        spark = (SparkSession.builder.appName("Linear_Regression")
            .master("spark://spark-master:7077")
            .getOrCreate())
        logging.info('Spark session successfully created')
    except Exception as e:
        traceback.print_exc(file=sys.stderr)  # To see traceback of the error.
        logging.error(f"Couldn't create the spark session due to exception: {e}")
        exit(0)

    """
    Initialisation de Minio pour récupérer les données déjà traités
    On teste d'abord l'existence du bucket
    """
    minio_client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )

    bucket = "compteurbucket"

    found = minio_client.bucket_exists(bucket)
    if not found:
        print("Bucket " + bucket + " n'existe pas; arrêt de l'entrainement")
        spark.stop()
    else:
        print("Bucket " + bucket + " existant")

    """
    Récupérer le fichier CSV qui se trouve dans votre bucket Warehouse
    """
    obj: urllib3.response.HTTPResponse = minio_client.get_object(
        'compteurbucket',  # Bucket
        'compteurs',  # Fichier CSV
    )

    """
    Préparation du Spark pour la lecture du fichier CSV vers un DataFrame Spark.
    Les traitements appliqués : Lecture des nom de colonnes, lecture du format utf-8 et
    garder les retours chariots pour garder les lignes du csv distincts
    """
    content = obj.data.decode('utf-8')
    lines = content.splitlines(keepends=True)
    rdd = spark.sparkContext.parallelize(lines)
    df_spark = spark.read.option("header", True).option("inferSchema", True).csv(rdd)
    df_spark = df_spark.dropna()

    """
    Si jamais une de vos colonnes sont des Dates, il faudra les convertir en nombre entier.
    Pour cela, lister les colonnes à convertir.
    Dans le cas contraire où vous n'avez pas besoin de convertir, il suffit de commenter à l'aide des #
    """
    for col in ["Timestamp"]:
        df_spark = df_spark.withColumn(col + "_int", unix_timestamp(col).cast("int"))

    """
    Diviser le DataFrame en deux sous DataFrame, à savoir train_data et validation_data en
    utilisation la méthode set_train_and_validation_ds
    """
    train_data, validation_data = set_train_and_validation_ds(df_spark, seed)

    """
    features_cols : La liste des colonnes du dataset dont vous allez utiliser pour entrainer le modèle
    target_col : Le nom de la colonne que vous allez prédire
    """
    features_cols = ['Timestamp', 'voltage', 'compteur_id', 'current', 'power_factor', 'consumption_KW', 'price', 'id_Machine', 'id_consumer', 'Nbr_Person', 'Nbr_machine']
    target_col = 'consumption_KW'


    assembler = VectorAssembler(inputCols=features_cols, outputCol="features")

    """
    Appliquer la transformation d'Assembler pour le DataFrame train_data
    """
    data_with_features = assembler.transform(train_data)


    lr = create_model(iter=100, features_cols="features", labelCol=target_col)


    model = lr.fit(data_with_features)

    print("Coefficients: " + str(model.coefficients))
    print("Intercept: " + str(model.intercept))

    save_model(model)


if __name__ == '__main__':
    main()