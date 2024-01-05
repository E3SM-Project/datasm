#!/bin/bash

scriptname=`basename $0`
usage="$scriptname <list_of_dsids_to_validate> [<alternative_dataset_spec.yaml>]"

if [ $# -eq 0 ]; then
    echo "Usage: $usage"
    exit 0
fi

tools=`$DSM_GETPATH STAGING_TOOLS`
validate=$tools/run_datasm_LocalEnv_validation_dsid_list_serially.sh

dsidlist=$1
alt_spec=$2

# echo "$1 $2"

if [ $# -eq 1 ]; then
    $validate $dsidlist
else
    $validate $dsidlist $alt_spec
fi

