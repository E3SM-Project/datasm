#!/bin/bash

dsidlist=$1

dsm_tools=`$DSM_GETPATH STAGING_TOOLS`
first_file=$dsm_tools/first_file_for_latest_by_dsid.sh

for dsid in `cat $dsidlist`; do
    $first_file $dsid
done
    
