from pyspark.sql import *
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import *

# Create a Spark session
spark = (SparkSession.builder.appName("Kafka_DataPreparation")
        .master("spark://spark-master:7077")
        .getOrCreate())
# Read data from Kafka topic

schema = StructType([
    StructField("compteur_id", IntegerType(), True),
    StructField("voltage", DoubleType(), True),
    StructField("current", DoubleType(), True),
    StructField("power_factor", DoubleType(), True),
    StructField("consumption_KW", DoubleType(), True),
    StructField("price", DoubleType(), True),
    StructField("id_Machine", DoubleType(), True),
    StructField("id_consumer", DoubleType(), True),
    StructField("Nbr_Person", DoubleType(), True),
    StructField("Nbr_machine", DoubleType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:2909") \
    .option("subscribe", "send_compteurdata_kafka") \
    .load()

# Perform data preparation
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data"))

# Select individual columns from parsed JSON data
preprocessed_df = parsed_df.withColumn('power_factor', when(parsed_df.data.power_factor >= 0, parsed_df.data.power_factor).otherwise(0)) \
    .withColumn('compteur_id', when(parsed_df.data.compteur_id.isNull(), 0).otherwise(parsed_df.data.compteur_id)) \
    .withColumn('voltage', when(parsed_df.data.voltage.isNull(), 0).otherwise(parsed_df.data.voltage)) \
    .withColumn('current', when(parsed_df.data.current >= 0, parsed_df.data.current).otherwise(0)) \
    .withColumn('consumption_KW', when(parsed_df.data.consumption_KW.isNull(), 0).otherwise(parsed_df.data.consumption_KW)) \
    .withColumn('price', when(parsed_df.data.price.isNull(), 0).otherwise(parsed_df.data.price)) \
    .withColumn('id_Machine', when(parsed_df.data.id_Machine.isNull(), 0).otherwise(parsed_df.data.id_Machine)) \
    .withColumn('id_consumer', when(parsed_df.data.id_consumer.isNull(), 0).otherwise(parsed_df.data.id_consumer)) \
    .withColumn('Nbr_Person', when(parsed_df.data.Nbr_Person.isNull(), 0).otherwise(parsed_df.data.Nbr_Person)) \
    .withColumn('Nbr_machine', when(parsed_df.data.Nbr_machine.isNull(), 0).otherwise(parsed_df.data.Nbr_machine))

# Write processed data to MinIO
preprocessed_df.writeStream \
    .format("parquet") \
    .option("path", "s3a://kafka-compteurbucket/processed_data") \
    .option("checkpointLocation", "/path/to/checkpoint/dir") \
    .start()

# Start the streaming job
spark.streams.awaitAnyTermination()

# Stop the Spark session
spark.stop()

