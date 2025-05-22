from kafka import KafkaConsumer
from pymongo import MongoClient, UpdateOne
import json
from datetime import datetime
import os
from bson import ObjectId

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'user_profiles')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
MONGO_DB = os.getenv('MONGO_DB', 'user_database')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'users')


def create_mongo_connection():
    """Create and return MongoDB connection and collection objects with index"""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    # Create index on 'id' field if it doesn't exist
    collection.create_index([('id', 1)], unique=True)
    return client, collection


def process_message(message_value, collection):
    """Process a single Kafka message and upsert data into MongoDB"""
    try:
        # Parse the JSON message
        message_data = json.loads(message_value)
        user_data = message_data.get('profiles', [])

        if not user_data:
            print(f"No 'profiles' array found in message generated at {message_data.get('generated_at')}")
            return

        # Prepare bulk operations for upsert
        bulk_operations = []
        existing_count = 0
        new_count = 0

        for user in user_data:
            user_id = user.get('id')
            if not user_id:
                print("Skipping document without 'id' field")
                continue

            # Create upsert operation
            bulk_operations.append(
                UpdateOne(
                    {'id': user_id},
                    {'$setOnInsert': user},  # Only set on insert, don't update existing
                    upsert=True
                )
            )

        if bulk_operations:
            # Execute bulk write
            result = collection.bulk_write(bulk_operations, ordered=False)

            print(
                f"Processed {len(user_data)} users from batch generated at {message_data.get('generated_at')} | "
                f"Inserted: {result.upserted_count} | "
                f"Existing: {len(user_data) - result.upserted_count}"
            )

    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")


def consume_from_kafka():
    """Consume messages from Kafka and process them"""
    # Create Kafka consumer with optimized settings
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        value_deserializer=lambda x: x.decode('utf-8'),
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        group_id='mongo_loader_group',
        max_poll_records=100,  # Process up to 100 messages at a time
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000
    )

    partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
    print(f"Consuming from {len(partitions)} partitions: {partitions}")

    # Create MongoDB connection
    mongo_client, collection = create_mongo_connection()

    try:
        print(f"Starting consumer for topic {KAFKA_TOPIC}...")
        while True:
            # Batch poll for better performance
            messages = consumer.poll(timeout_ms=1000)

            for topic_partition, msg_list in messages.items():
                print(f"Processing {len(msg_list)} messages from partition {topic_partition.partition}")

                for message in msg_list:
                    process_message(message.value, collection)

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()
        mongo_client.close()
        print("Consumer stopped. Connections closed.")


if __name__ == "__main__":
    consume_from_kafka()