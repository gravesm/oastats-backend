#!/usr/bin/env bash

MONGO_HOST=
MONGO_DB=oastats
MONGO_REQ=requests
MONGO_SUM=summary
START_DATE=$(date -d yesterday +"%Y-%m-01")
END_DATE=$(date -d "$START_DATE +1 month")

START_EPOCH=$(date -d "$START_DATE" +"%s000")
END_EPOCH=$(date -d "$END_DATE" +"%s000")

mongodump --host $MONGO_HOST --db $MONGO_DB --collection $MONGO_REQ --query \
"{\"time\": {\"\$gte\": new Date($START_EPOCH), \"\$lt\": new Date($END_EPOCH)}}"

mongodump --host $MONGO_HOST --db $MONGO_DB --collection $MONGO_SUM
