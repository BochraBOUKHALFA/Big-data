from kafka import KafkaConsumer
import json
from minio import Minio
from minio.error import S3Error
import io

def main():
    # Initialize MinIO client
    minioClient = Minio('localhost:9000',
                        access_key='minio',
                        secret_key='minio123',
                        secure=False)

    # Check if the bucket exists, create it if not
    bucket_name = 'catbucket'
    found = minioClient.bucket_exists(bucket_name)
    if not found:
        minioClient.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")

    # Initialize Kafka consumer
    consumer = KafkaConsumer('sending_cats',
                            bootstrap_servers=['localhost:9092'],
                            value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    # Infinite loop to read Kafka data and save it to MinIO
    for message in consumer:
        data = message.value

        # Convert data to dictionary
        data_dict = {
            'Timestamp': data[0],
            'Name': data[1],
            'Age': data[2],
            'Breed': data[3]
        }

        # Define the object name
        object_name = f"{data_dict['Timestamp']}.json"

        # Encode the data as JSON
        json_data = json.dumps(data_dict)

        try:
            # Calculate the length of the data
            data_length = io.BytesIO(json_data.encode('utf-8')).getbuffer().nbytes

            # Save the data to MinIO bucket
            minioClient.put_object(bucket_name, object_name, io.BytesIO(json_data.encode('utf-8')), data_length)
            print(f"Saved object: {object_name} to bucket: {bucket_name}")
        except S3Error as e:
            print(f"Error saving object: {object_name} to bucket: {bucket_name}")
            print(f"Error details: {e}")

if __name__ == '__main__':
    main()