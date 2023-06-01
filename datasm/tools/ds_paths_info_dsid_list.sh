#!/bin/bash

dsidlist=$1

for dsid in `cat $dsidlist`; do
    echo " "
    /p/user_pub/e3sm/staging/tools/ds_paths_info.sh $dsid
done
