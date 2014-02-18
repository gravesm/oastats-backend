#!/usr/bin/env python

import os

os.environ.setdefault("OASTATS_SETTINGS", "pipeline.settings")

from pipeline.conf import settings
from pipeline.load_json import get_collection
import pipeline.summarize as summarize

def main():
    requests = get_collection(settings.MONGO_DB,
                              settings.MONGO_COLLECTION,
                              settings.MONGO_CONNECTION)
    summary = get_collection(settings.MONGO_DB,
                             settings.MONGO_SUMMARY_COLLECTION,
                             settings.MONGO_CONNECTION)

    summarize.add_summary_data(requests, summary)
    summarize.add_summary_map(requests, summary)
    summarize.add_summary_time(requests, summary)

if __name__ == '__main__':
    main()