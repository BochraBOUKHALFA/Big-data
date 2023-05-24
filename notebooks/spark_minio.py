from pyspark.sql import SparkSession

# Create a Spark session
spark = (SparkSession.builder.appName("DataPreparation")
        .master("spark://spark-master:7077")
        .getOrCreate())

# Read data from MinIO
df = spark.read.format("csv") \
    .option("header", "true") \
    .load("s3a://your-bucket/your-data.csv")

preprocessed_df = parsed_df.withColumn('power_factor', when(parsed_df['data.power_factor'] >= 0, parsed_df['data.power_factor']).otherwise(0)) \
    .withColumn('compteur_id', when(parsed_df['data.compteur_id'].isNull(), 0).otherwise(parsed_df['data.compteur_id'])) \
    .withColumn('voltage', when(parsed_df['data.voltage'].isNull(), 0).otherwise(parsed_df['data.voltage'])) \
    .withColumn('current', when(parsed_df['data.current'].isNull(), 0).otherwise(parsed_df['data.current'])) \
    .withColumn('consumption_KW', when(parsed_df['data.current'].isNull(), 0).otherwise(parsed_df['data.current'])) \
    .withColumn('price', when(parsed_df['data.current'].isNull(), 0).otherwise(parsed_df['data.current'])) \
    .withColumn('id_Machine', when(parsed_df['data.current'].isNull(), 0).otherwise(parsed_df['data.current'])) \
    .withColumn('id_consumer', when(parsed_df['data.current'].isNull(), 0).otherwise(parsed_df['data.current'])) \
    .withColumn('Nbr_Person', when(parsed_df['data.current'].isNull(), 0).otherwise(parsed_df['data.current'])) \
    .withColumn('Nbr_machine', when(parsed_df['data.current'].isNull(), 0).otherwise(parsed_df['data.current']))

# Write data to MinIO
df.write.format("parquet") \
    .mode("overwrite") \
    .save("s3a://your-bucket/output-data.parquet")

# Stop the Spark session
spark.stop()