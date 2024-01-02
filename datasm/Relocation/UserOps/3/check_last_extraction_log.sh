#!/bin/bash

locallog=`ls | grep runlog_archive_extraction | wc -l`

echo "locallog = $locallog"

if [ $locallog -eq 0 ]; then
    cd extraction_logs
fi

lastlog=`ls | grep runlog_archive_extraction | tail -1`
# cat $lastlog | grep -v zstash | grep -v Begin | grep -v Conducting | grep -v Created
cat $lastlog | grep -v zstash | grep -v Begin | grep -v Conducting 

