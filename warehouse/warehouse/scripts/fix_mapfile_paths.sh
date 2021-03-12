#!/bin/bash

if [ $# -lt 1 ] || [ $1 == "-h" ]; then
    echo "Usage: fix_mapfile_paths.sh mapfile_full_path"
    exit 0
fi

mapfile=$1

if ! [ -f $mapfile ]; then
    echo "ERROR: File not found: $mapfile"
    exit 1
fi

# WARNING: HARDCODED STRINGS
src_pattern="e3sm\/warehouse"
dst_pattern="work"

sed -i "s/$src_pattern/$dst_pattern/g" $mapfile

exit 0






