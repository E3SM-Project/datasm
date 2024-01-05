#!/bin/bash

# usage: ./$0 full_path_to_zstash_archive
if [ $# -eq 0 ]; then
    echo "usage:  ./$0 full_path_to_archive"
    exit 0
fi

arch_path=$1

tools=`$DSM_GETPATH STAGING_TOOLS`
python $tools/archive_holodeck_setup.py -A $arch_path

