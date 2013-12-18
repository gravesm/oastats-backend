#!/usr/bin/env python

from pymongo import MongoClient
import fileinput
import json
from datetime import datetime
import time
from conf import settings


def get_collection(db, collection, conn):
    client = MongoClient(*conn)
    return client[db][collection]

def insert(collection, request):
    collection.insert(request)

def main():
    collection = get_collection(settings.MONGO_DB,
                                settings.MONGO_COLLECTION,
                                settings.MONGO_CONNECTION)
    for line in fileinput.input():
        request = json.loads(line)
        t = time.strptime(request['time'].split(' ')[0], "[%d/%b/%Y:%H:%M:%S")
        request['time'] = datetime.fromtimestamp(time.mktime(t))
        insert(collection, request)

if __name__ == '__main__':
    main()
