#!/bin/bash

dsid=$1

dsm_tools=`$DSM_GETPATH STAGING_TOOLS`
latest_path=$dsm_tools/latest_data_location_by_dsid.sh

verdir=`$latest_path $dsid`
if [ ! -d $verdir ]; then
    echo "NONE"
    continue
fi

firstfile=`ls $verdir | head -1 2>/dev/null`
echo "$firstfile"
    
