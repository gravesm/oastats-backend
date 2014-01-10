from pymongo import MongoClient

def get_collection(db, collection, conn):
    """Return specified MongoDB collection.

    conn should be an empty tuple, a tuple that contains host and port, or a
    tuple that contains a MongoDB URI.

    """
    client = MongoClient(*conn)
    return client[db][collection]

def insert(collection, request):
    """Insert a request dictionary into the specified MongoDB collection."""
    collection.insert(request)
