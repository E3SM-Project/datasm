#!/bin/bash

scriptname=`basename $0`
usage="$scriptname <list_of_dsids_to_publish> [<alternative_dataset_spec.yaml>]"

if [ $# -eq 0 ]; then
    echo "Usage: $usage"
    exit 0
fi

tools=`$DSM_GETPATH STAGING_TOOLS`
publish=$tools/run_datasm_LocalEnv_publish_dsid_list_serially.sh

dsidlist=$1
alt_spec=$2

# echo "$1 $2"

$publish $dsidlist $alt_spec




