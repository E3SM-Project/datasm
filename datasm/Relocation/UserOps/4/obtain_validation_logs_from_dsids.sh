#!/bin/bash

dsids=$1

for dsid in `cat $dsids`; do
    lastlog=`ls validation_logs | grep $dsid | tail -1`
    # echo "LASTLOG = $lastlog"
    if [ -f validation_logs/$lastlog ]; then
        echo "validation_logs/$lastlog"
    else
        echo "$dsid: No Validation_Log found"
    fi
done

