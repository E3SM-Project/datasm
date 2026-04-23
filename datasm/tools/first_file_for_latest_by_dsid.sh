#!/bin/bash

dsid=$1

do_full=0
if [[ $# -eq 2 ]]; then
    if [[ $2 == "FULLPATH" ]]; then
        do_full=1
    fi
fi

dsm_tools=`$DSM_GETPATH STAGING_TOOLS`
latest_path=$dsm_tools/latest_data_location_by_dsid.sh

verdir=`$latest_path $dsid`
if [ ! -d $verdir ]; then
    echo "NONE"
    exit 0
fi

firstfile=`ls $verdir | head -1 2>/dev/null`

if [[ $do_full -eq 1 ]]; then
    echo "$verdir/$firstfile"
else
    echo "$firstfile"
fi


    
