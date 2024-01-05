#!/bin/bash

# NOTE: assumes new directory and updated mapfile are in place.

dsids=$1

get_path_info=`$DSM_GETPATH STAGING_TOOLS`/ds_paths_info_compact.sh

for dsid in `cat $dsids`; do
    ts=`date -u +%Y%m%d_%H%M%S_%6N`
    $get_path_info $dsid > ztmp_info
    statfile=`cat ztmp_info | grep SF_PATH | cut -f2 -d' '`
    pub_path=`cat ztmp_info | grep PB_PATH | cut -f2 -d' ' | cut -f1 -d:`
    map_file=$pub_path/${dsid}.map

    pub_log="Publication_Log-$ts-$dsid"
    pub_cmd="esgpublish --map $map_file"

    echo $pub_cmd > $pub_log 2>&1
    $pub_cmd > $pub_log 2>&1
    retcode=$?

    ts=`date -u +%Y%m%d_%H%M%S_%6N`
    if [ $retcode -eq 0 ]; then
        stat_msg="STAT:$ts:PUBLICATION:Pass"
    else
        stat_msg="STAT:$ts:PUBLICATION:Fail"
    fi
    echo $stat_msg
    echo $stat_msg >> $pub_log
    echo $stat_msg >> $statfile
done   


