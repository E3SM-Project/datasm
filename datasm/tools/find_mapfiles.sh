#!/bin/bash

dsidlist=$1

tools=`$DSM_GETPATH STAGING_TOOLS`

latest=$tools/latest_data_location_by_dsid.sh

# seeking parent_dir/.mapfile-v20250728.map

for dsid in `cat $dsidlist`; do
    lastverdir=`$latest $dsid`
    lastver=`basename $lastverdir`
    parent_dir=`dirname $lastverdir`
    testvalue=$parent_dir/.mapfile-${lastver}.map
    if [[ ! -f $testvalue ]]; then
        echo "$dsid:NONE"
    else
        echo "$dsid:FOUND:$testvalue"
    fi
done
