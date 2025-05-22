from kafka import (KafkaConsumer)
from pymongo import MongoClient, UpdateOne
import json
import os


KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'user_profiles')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
MONGO_DB = os.getenv('MONGO_DB', 'user_database')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'users')


def create_mongo_connection():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    collection.create_index([('id', 1)], unique=True)
    return client, collection


def process_message(message_value, collection):
    try:
        message_data = json.loads(message_value)
        user_data = message_data.get('profiles', [])

        if not user_data:
            print(f"No 'profiles' array found in message generated at {message_data.get('generated_at')}")
            return

        bulk_operations = []

        for user in user_data:
            user_id = user.get('id')
            if not user_id:
                print("Skipping document without 'id' field")
                continue

            bulk_operations.append(
                UpdateOne(
                    {'id': user_id},
                    {'$setOnInsert': user},
                    upsert=True
                )
            )

        if bulk_operations:
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
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        value_deserializer=lambda x: x.decode('utf-8'),
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        group_id='mongo_loader_group',
        max_poll_records=100,
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000
    )

    partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
    print(f"Consuming from {len(partitions)} partitions: {partitions}")

    mongo_client, collection = create_mongo_connection()

    try:
        print(f"Starting consumer for topic {KAFKA_TOPIC}...")
        while True:
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