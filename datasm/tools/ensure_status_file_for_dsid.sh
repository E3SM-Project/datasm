#!/bin/bash

dsid=$1

sf_root1=`$DSM_GETPATH STAGING_STATUS`
staging=`$DSM_GETPATH DSM_STAGING`
sf_root2=$staging/status_ext

if [ $project == "CMIP6" ]; then
    instid=`echo $dsid | cut -f3 -d.`
    if [[ $instid == "E3SM-Project" || $instid == "UCSB" ]]; then
        sf_root=$sf_root1
    else
        sf_root=$sf_root2
    fi
fi

fullpath="$sf_root/${dsid}.status"

if [ ! -f $fullpath ]; then
    echo "DATASETID=$dsid" >> $fullpath
fi

echo $fullpath
