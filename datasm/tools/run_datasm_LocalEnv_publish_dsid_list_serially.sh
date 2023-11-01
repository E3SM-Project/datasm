#!/bin/bash

usage="<file_of_warehouse_dataset_ids> [<path_to_alternate_dataset_spec>]"

if [ $# -eq 0 ]; then
    scriptname=`basename $0`
    echo "Usage: [STAGING_TOOLS]/$scriptname $usage"
    exit 0
fi

inlist=$1

ds_spec=`$DSM_GETPATH STAGING_RESOURCE`/dataset_spec.yaml

if [ $# -eq 2 ]; then
    ds_spec=$2
fi

thisdir=`pwd`
workdir=`realpath $thisdir`

for dsid in `cat $inlist`; do
    if [ ${dsid:0:1} == "#" ]; then
        continue
    fi

    ts=`date -u +%Y%m%d_%H%M%S_%6N`
    pub_log=$workdir/Publication_Log-$ts-$dsid

    echo " "
    echo " " >> $pub_log
    echo "$ts: preparing to publish: $dsid (dataset_spec = $ds_spec)" >> $pub_log

    workers=12

    the_cmd="datasm publish --job-workers $workers -d $dsid --dataset-spec $ds_spec"
    echo "ISSUING:  $the_cmd"
    echo "ISSUING:  $the_cmd" >> $pub_log

    $the_cmd > $pub_log 2>&1
    retcode=$?

    ts=`date -u +%Y%m%d_%H%M%S_%6N`
    echo "$ts: retcode = $retcode" >> $pub_log

    mv slurm_scripts slurm_scripts-$ts-$dsid

done




