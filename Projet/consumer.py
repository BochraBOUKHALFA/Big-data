from kafka import KafkaConsumer
import json
from minio import Minio
from minio.error import S3Error
import io

def main():
    minioClient = Minio('localhost:9000',
                        access_key='minio',
                        secret_key='minio123',
                        secure=False)

    # Check if the bucket exists, create it if not
    bucket_name = 'kafka-compteurbucket'
    found = minioClient.bucket_exists(bucket_name)
    if not found:
        minioClient.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")

    # Initialize Kafka consumer
    consumer = KafkaConsumer('send_compteur_data',
                            bootstrap_servers=['localhost:9092'],
                            value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    # Infinite loop to read Kafka data and save it to MinIO
    for message in consumer:
        data = message.value

        # Convert data to dictionary
        data_dict = {
            "compteur_id": data["compteur_id"],
            "voltage": data["voltage"],
            "current": data["current"],
            "power_factor": data["power_factor"],
            "timestamp": data["timestamp"],
            'consumption_KW': data["consumption_KW"],
            'price': data["price"],
            'id_Machine': data["id_Machine"],
            'id_consumer': data["id_consumer"],
            'Nbr_Person': data["Nbr_Person"],
            'Nbr_machine': data["Nbr_machine"]
        }

        object_name = f"{data_dict['timestamp']}.json"

        # Encode the data as JSON
        json_data = json.dumps(data_dict)

        try:
            data_length = io.BytesIO(json_data.encode('utf-8')).getbuffer().nbytes

            # Save the data to MinIO bucket
            minioClient.put_object(bucket_name, object_name, io.BytesIO(json_data.encode('utf-8')), data_length)
            print(f"Saved object: {object_name} to bucket: {bucket_name}")
        except S3Error as e:
            print(f"Error saving object: {object_name} to bucket: {bucket_name}")
            print(f"Error details: {e}")

if __name__ == '__main__':
    main()