from kafka import KafkaConsumer
from pymongo import MongoClient
import json
from datetime import datetime
import os

# # Configuration from environment variables
# KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
# KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
# MONGO_URI = os.getenv('MONGO_URI')
# MONGO_DB = os.getenv('MONGO_DB')
# MONGO_COLLECTION = os.getenv('MONGO_COLLECTION')

KAFKA_BOOTSTRAP_SERVERS = "95.164.113.111:9092"
KAFKA_TOPIC = "vk_users"
MONGO_URI = "mongodb://admin:supersecret@95.164.113.111:27017/"
MONGO_DB = "Database"
MONGO_COLLECTION = "vk_users"


def create_mongo_connection():
    """Create and return MongoDB connection and collection objects"""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    return client, collection


def process_message(message_value, collection):
    """Process a single Kafka message and insert data into MongoDB"""
    try:

        message_data = json.loads(message_value)

        user_data = message_data.get('profiles', [])

        if not user_data:
            print(f"No 'data' array found in message generated at {message_data.get('generated_at')}")
            return

        # Prepare documents for insertion
        documents = []
        for user in user_data:
            documents.append(user)

        result = collection.insert_many(documents)
        print(
            f"Inserted {len(result.inserted_ids)} documents from batch generated at {message_data.get('generated_at')}")

    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")


def consume_from_kafka():
    """Consume messages from Kafka and process them"""
    # Create Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',  # or 'latest' depending on your needs
        value_deserializer=lambda x: x.decode('utf-8'),
        enable_auto_commit=True,
        group_id='mongo_loader_group'
    )

    partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
    print(f"Consuming from {len(partitions)} partitions: {partitions}")

    # Create MongoDB connection
    mongo_client, collection = create_mongo_connection()

    try:
        print(f"Starting consumer for topic {KAFKA_TOPIC}...")
        for message in consumer:
            print(f"Received message from partition {message.partition}")
            process_message(message.value, collection)
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()
        mongo_client.close()
        print("Consumer stopped. Connections closed.")

if __name__ == "__main__":
    consume_from_kafka()
