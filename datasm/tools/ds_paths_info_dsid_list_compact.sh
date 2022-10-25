#!/bin/bash

dsidlist=$1

for dsid in `cat $dsidlist`; do
    echo " "
    ~/.w/ds_paths_info_compact.sh $dsid
done
