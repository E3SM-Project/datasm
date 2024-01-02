#!/bin/bash

this_script=`basename $0`

usage="$this_script file_of_dataset_ids [updatestatus]"

if [ $# -eq 0 ]; then
    echo "Usage: $usage"
    exit 0
fi

inlist=$1
updatestatus=0

if [ $# -eq 2 ]; then
    if [ $2 == "updatestatus" ]; then
        updatestatus=1
    fi
fi

if [ $updatestatus -eq 1 ]; then
    echo Update Status
else
    echo Testing Status
fi

verify=`$DSM_GETPATH STAGING_TOOLS`/datasm_verify_publication.py

if [ $updatestatus -eq 1 ]; then
    python $verify -i $inlist --update-status
else
    python $verify -i $inlist
fi
 
