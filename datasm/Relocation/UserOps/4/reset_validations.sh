#!/bin/bash

text="Remove all warehouse ensemble version directories above v0"

dsidlist=$1

dsetpi=`$dsm_GETPATH STAGING_TOOLS`/ds_paths_info.sh

ts=`date -u +%Y%m%d_%H%M%S_%6N`


for dsid in `cat $dsidlist`; do

    wh_path=`$dsetpi $dsid | grep WH_PATH | cut -f2 -d' '`
    if [ ! -d $wh_path ]; then
        echo "${ts}:ERROR: No warehouse path found for dataset id: $dsid"
        continue
    fi
    vdirs=`ls $wh_path`
    # echo ""
    # echo $dsid
    for vdir in $vdirs; do
        num=`ls $wh_path/$vdir | wc -l`
        # echo "  $vdir:  $num"
        if [[ $vdir == "v0" && $num -gt 0 ]]; then
            continue
            # echo "skipping populated v0 ($num files)"
        fi
        if [[ $vdir != "v0" ]]; then
            echo "exec rm -rf $wh_path/$vdir"
            rm -rf $wh_path/$vdir
        fi
    done

done
