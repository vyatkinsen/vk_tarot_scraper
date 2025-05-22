from pymongo import MongoClient
from pymongo.errors import PyMongoError

from KafkaToMongo import MONGO_DB


def remove_duplicates():
    # Connect to MongoDB
    CLIENT = MongoClient('mongodb://admin:supersecret@95.164.113.111:27017/')  # Update with your MongoDB URI
    MONGO_DB = CLIENT['Database']  # Update with your database name
    MONGO_COLLECTION = MONGO_DB['vk_users']  # Update with your collection name

    # Define the field that determines uniqueness
    unique_field = "id"  # Change to your unique field

    try:
        # Step 1: Find duplicates using aggregation
        print("start group by")
        pipeline = [
            {
                "$group": {
                    "_id": f"${unique_field}",
                    "dups": {"$push": "$_id"},
                    "count": {"$sum": 1}
                }
            },
            {
                "$match": {
                    "count": {"$gt": 1}
                }
            }
        ]

        duplicates = MONGO_COLLECTION.aggregate(pipeline)
        print("ending group by")

        # Step 2: Delete duplicates (keeping first occurrence)
        deleted_count = 0
        print((duplicates.__sizeof__()))
        for doc in duplicates:
            # Keep first document, remove others
            ids_to_remove = doc['dups'][1:]  # Skip first element
            print(ids_to_remove)
            result = MONGO_COLLECTION.delete_many({"_id": {"$in": ids_to_remove}})
            deleted_count += result.deleted_count

        print(f"Removed {deleted_count} duplicate documents")

    except PyMongoError as e:
        print(f"An error occurred: {e}")
    finally:
        CLIENT.close()


if __name__ == "__main__":
    remove_duplicates()