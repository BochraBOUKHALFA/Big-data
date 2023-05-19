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
        spark = SparkSession.builder.appName("LinearRegressionExample").getOrCreate()
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

    bucket = "warehouse"

    found = minio_client.bucket_exists(bucket)
    if not found:
        print("Bucket "+ bucket +" n'existe pas; arrêt de l'entrainement")
        spark.stop()
    else:
        print("Bucket " + bucket + " existant")

    """
    Récupérer le fichier CSV qui se trouve dans votre bucket Warehouse
    """
    obj: urllib3.response.HTTPResponse = minio_client.get_object(
        '???', # Bucket
        '', # Fichier CSV
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
    for col in ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
                "order_estimated_delivery_date", "order_delivered_customer_date"]:
        df_spark = df_spark.withColumn(col + "_int", unix_timestamp(col).cast("int"))

    """
    Diviser le DataFrame en deux sous DataFrame, à savoir train_data et validation_data en
    utilisation la méthode set_train_and_validation_ds
    """
    ?, ? = ???(df_spark, seed)

    """
    features_cols : La liste des colonnes du dataset dont vous allez utiliser pour entrainer le modèle
    target_col : Le nom de la colonne que vous allez prédire
    """
    features_cols = ["order_purchase_timestamp_int", "order_approved_at_int",
                     "order_delivered_carrier_date_int", "order_estimated_delivery_date_int"]
    target_col = "order_delivered_customer_date_int"

    '''
    Assembler = Spécifique à SparkML, permet de mettre les colonnes d'entrainement sous la forme d'une seule colonne
    Prends en inputCols les noms des colonnes à transformer
    Prends en outputCol le nom "features"
    '''
    assembler = VectorAssembler(inputCols=???, outputCol=???)

    """
    Appliquer la transformation d'Assembler pour le DataFrame train_data
    """
    data_with_features = assembler.transform(???)

    """
    Création d'un modèle de Régression Linéaire avec: 
        - le nombre d'itération maximale pour trouver les paramètres optimaux
        - Le nom de la colonne servant pour l'entrainement (features)
        - Le nom de la colonne à prédire (target_col)
    """
    lr = create_model(iter=???, features_cols="???", labelCol=???)

    """
    Une fois que le modèle est créée, Lancement de l'entrainement.
    Trouvez la fonction qui permet de lancer l'entrainement avec le data_with_feature en paramètre
    """
    model = lr.???(???)

    """
    Affichage des coefficients trouvés durant la phase de l'entrainement
    Affichage de l'interception de la courbe de la régression linéaire
    """
    print("Coefficients: " + str(model.coefficients))
    print("Intercept: " + str(model.intercept))

    """
    Maintenant il faut sauvegarder le modèle
    """
    save_model(???)


if __name__ == '__main__':
    main()
