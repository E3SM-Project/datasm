#!/bin/bash

usage="run_datasm_validation_list_dsids_serially.sh file_of_warehouse_dataset_dsids(ensembles) [spec=<alt_dataset_spec.yaml>]"

inlist=$1

here=`pwd`
workdir=`realpath $here`

ds_spec=`$DSM_GETPATH STAGING_RESOURCE`/dataset_spec.yaml
trim=0

i=1
while [ $i -le $# ]; do
    if [[ ${!i:0:5} == "spec=" ]]; then
        ds_spec=${!i:5}
        # echo SPEC=$ds_spec
    fi
    ((++i))
done

for dsid in `cat $inlist`; do
    if [ ${dsid:0:1} == "#" ]; then
        continue
    fi

    ts=`date -u +%Y%m%d_%H%M%S_%6N`
    val_log=$workdir/Validation_Log-$ts-$dsid
    # the_cmd="datasm validate --job-workers 80 -d $dsid"
    the_cmd="datasm validate --job-workers 80 -d $dsid --dataset-spec $ds_spec"
    echo Issuing: $the_cmd >> $val_log 2>&1

    # execute the command
    $the_cmd >> $val_log 2>&1
    retcode=$?
    echo "retcode = $retcode" >> $val_log 2>&1

    ts=`date -u +%Y%m%d_%H%M%S_%6N`
    mv $workdir/slurm_scripts $workdir/slurm_scripts-$ts-$dsid

done 

