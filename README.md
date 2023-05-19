# Squelette de projet Traitement de l'Intelligence Artificielle

Le but de ce projet est de mettre en œuvre une solution IA associé à l'IoT et le Big Data.
Le squelette est adapté à la classe I1 GR4

Les élèves utilisent tous Kafka pour envoyer les données du capteur vers un cluster Spark, par l'intermédiaire d'un Stockage minIO.

Il y a les programmes suivants :
1. Un Capteur
2. Un programme pour tourner Kafka
3. Un programme pour nettoyer les données de Kafka via Spark
4. Un programme pour entraîner un modèle de machine learning
5. Un programme pour prédire les résultats à partir du modèle entraîné

Rendu :
- Le code source
- Un rapport
- Éventuellement un support de présentation au format PDF

## Pour installer Kafka :
- Rendez-vous vers ce lien pour télécharger : https://www.conduktor.io/download/
- Créez votre compte (obligatoire) : 
- Une fois l'installation réalisée, lancez l'application et connectez-vous
- Suivez les instructions pour créer un Cluster Locale (Téléchargez la dernière version de Kafka)

## Pour installer un dataLake ( ici minIO type S3 ):

 - lancer la commande docker-compose up
 - Patientez
 - Patientez
 - Patientez
 - Quand c'est fini : 
   - Ouvrez `Docker Desktop` et ouvrez l'URL sur le conteneur minIO 
     - Pour se connecter c'est `minio` et `minio123`
     - vous avez une bucket de deja prête qui s'appel `warehouse`.
     - ouvrir le conteneur `spark-notebook` mais au niveau des logs:
       - Cliquez sur le lien qui ressemble à : `http://127.0.0.1:8888/lab?token=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX`
 
 - vous pouvez copier le code python sur les cellules jupyter
 
