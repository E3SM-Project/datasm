#!/bin/bash

if [[ $# -lt 1 ]]; then
    echo "usage:  $0 <e3sm_dataset_id>"
    exit 0
fi

e3sm_dsid=$1

tools=`$DSM_GETPATH STAGING_TOOLS`
get_first_file=$tools/first_file_for_latest_by_dsid.sh

$get_first_file $e3sm_dsid | cut -f1-3 -d.
