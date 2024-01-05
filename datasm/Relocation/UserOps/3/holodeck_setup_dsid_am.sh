#!/bin/bash

if [ $# -eq 0 ]; then
    echo "usage:  ./$0 <native_dataset_id> [am=<alternative_archive_map>]"
    exit 0
fi

dsid=$1

arch_map=`$DSM_GETPATH ARCHIVE_MANAGEMENT/Archive_Map

if [ $# -eq 2 ]; then
    arg2=$2
    if [ ${arg2:0:3} == "am=" ]; then
        slen=${#arg2}
        arch_map=${arg2:3: $slen}
    fi
fi

arch_path=`grep $dsid $arch_map | cut -f3 -d,`
file_patt=`grep $dsid $arch_map | cut -f4 -d,`

echo "FOUND arch_path: $arch_path"

tools=`$DSM_GETPATH STAGING_TOOLS`
python $tools/archive_holodeck_setup.py -A $arch_path

mv Holodeck Holodeck-$dsid

echo "Default Extraction Pattern: $file_patt"


