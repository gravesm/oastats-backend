#!/usr/bin/env bash

###
# This script is intended to automate the process of passing the
# previous day's requests from the Apache logs to the OA stats
# pipeline.
###

set -o pipefail

## Configurable variables ##

# Location of oastats.py. If using virtualenv, make sure to execute with correct
# python binary.
OASTATS_PIPELINE=

# Location of summary.py.
OASTATS_SUMMARY=

# Location of current Apache log.
CURRENT_LOG=

# Location of log archives. This will be prepended to the previous year/month
# when the previous month's log needs to be accessed.
LOG_ARCHIVE=

############################


YESTERDAY=$(date -d yesterday +%d/%b/%Y)
LAST_MONTH_LOG=

if [ $(date +%d) -eq 1 ]
then
    LAST_MONTH_LOG="${LOG_ARCHIVE}/$(date -d yesterday +%Y/%m)/access*"
fi

grep -h $YESTERDAY $CURRENT_LOG $LAST_MONTH_LOG | $OASTATS_PIPELINE

STATUS=$?

if [ $STATUS -eq 0 ]
then
    $OASTATS_SUMMARY
else
    echo $(basename "$0"): Pipeline processing failed, skipping summary 1>&2
    exit $STATUS
fi
