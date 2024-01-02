#!/bin/bash

dsidlist=$1

dspaths=`$DSM_GETPATH STAGING_TOOLS`/ds_paths_info.sh

setcount=0

for dsid in `cat $dsidlist`; do
    wh_path=`$dspaths $dsid | grep WH_PATH | cut -f2 -d' '`
    pb_path=`$dspaths $dsid | grep PB_PATH | cut -f2 -d' '`
    wh_vers=`ls $wh_path | tail -1`
    pb_vers=`ls $pb_path | tail -1`
    wh_full=$wh_path/$wh_vers
    pb_full=$pb_path/$pb_vers

    mapfile=$pb_path/${dsid}.map
    # echo " "
    # echo $wh_full
    # echo $pb_full
    # echo $mapfile

    # echo "mv $pb_full/* $wh_full"
    # echo "rmdir $pb_full"
    # echo "rm $mapfile"

    mv $pb_full/* $wh_full
    rmdir $pb_full
    rm $mapfile

    setcount=$((setcount + 1))
done

echo "Reset $setcount datasets to warehouse"
    
