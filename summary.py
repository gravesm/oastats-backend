import os

os.environ.setdefault("OASTATS_SETTINGS", "settings.py")

from pipeline.conf import settings
from pipeline.load_json import get_collection
from pipeline.summarize import create_summary_collection

def main():

    requests = get_collection(settings.MONGO_DB,
                              settings.MONGO_COLLECTION,
                              settings.MONGO_CONNECTION)
    summary = get_collection(settings.MONGO_DB,
                             settings.MONGO_SUMMARY_COLLECTION,
                             settings.MONGO_CONNECTION)

    create_summary_collection(requests, summary)


if __name__ == '__main__':
    main()
