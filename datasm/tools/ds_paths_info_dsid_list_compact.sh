#!/bin/bash

dsidlist=$1

dsm_tools=`$DSM_GETPATH STAGING_TOOLS`

dspi_compact=$dsm_tools/ds_paths_info_compact.sh

for dsid in `cat $dsidlist`; do
    echo " "
    $dspi_compact $dsid
done
