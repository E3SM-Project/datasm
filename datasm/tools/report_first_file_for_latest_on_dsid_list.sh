#!/bin/bash

dsidlist=$1

latest_path=/p/user_pub/e3sm/staging/tools/latest_data_location_by_dsid.sh

for dsid in `cat $dsidlist`; do

    verdir=`$latest_path $dsid`
    if [ ! -d $verdir ]; then
        echo "$dsid:  No Path for data"
        continue
    fi

    firstfile=`ls $verdir | head -1 2>/dev/null`
    echo "$dsid:  $firstfile"
done
    
