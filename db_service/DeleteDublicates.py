from pymongo import MongoClient
from pymongo.errors import PyMongoError


def remove_duplicates():
    CLIENT = MongoClient('mongodb://admin:supersecret@95.164.113.111:27017/')
    MONGO_DB = CLIENT['Database']
    MONGO_COLLECTION = MONGO_DB['vk_users']

    unique_field = "id"

    try:
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

        deleted_count = 0
        print((duplicates.__sizeof__()))
        for doc in duplicates:
            ids_to_remove = doc['dups'][1:]
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