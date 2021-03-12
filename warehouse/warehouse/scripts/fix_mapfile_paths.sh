#!/bin/bash

if [ $# -lt 3 ] || [ $1 == "-h" ]; then
    echo "Usage: fix_mapfile_paths.sh mapfile_full_path warehouse_rootpath publication_rootpath"
    exit 0
fi

mapfile=$1
wh_base=$2
pub_base=$3

if ! [ -f $mapfile ]; then
    echo "ERROR: File not found: $mapfile"
    exit 1
fi

sed -i "s/$wh_base/$pub_base/g" $mapfile

exit 0






