#!/usr/bin/env bash

WORKDATE=$1

if [ -z "$WORKDATE" ]; then
    echo "Usage:$0 <work-date>"
    exit -1
fi

for INDUSTRY_CODE in 1100 2000 3110
do
    echo "`date +%H:%M:%S` - Loading user metrics for industry code $INDUSTRY_CODE ..."
    ./submit.sh $WORKDATE $INDUSTRY_CODE 1>submit.sh.${WORKDATE}.${INDUSTRY_CODE}.log 2>&1
    if [ $? -ne 0 ]; then
        echo "`date +%H:%M:%S` - Failed."
        exit -1
    else
        echo "`date +%H:%M:%S` - Finished."
        echo
    fi
done