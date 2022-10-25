#!/bin/bash

sf_root="/p/user_pub/e3sm/staging/status"

dsid=$1

fullpath="$sf_root/${dsid}.status"

if [ ! -f $fullpath ]; then
    echo "DATASETID=$dsid" >> $fullpath
fi

echo $fullpath
