#!/bin/bash

dsidlist=$1

dsm_tools=`$DSM_GETPATH STAGING_TOOLS`

for dsid in `cat $dsidlist`; do
    echo " "
    $dsm_tools/ds_paths_info.sh $dsid
done
