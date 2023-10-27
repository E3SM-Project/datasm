#!/bin/bash

# FORMAT
# generate_esgmapfile <num_processes> <dataset_fullpath> 
# Note: output mapfile fullpath will be <dataset_ens_path>.mapfile

ini_path=`$DSM_GETPATH DSM_STAGING`/ini_std/
log_path=./

proc_num=$1
dataset_fullpath=$2
dataset_ens_path=`echo $dataset_fullpath | rev | cut -f2- -d/ | rev`
out_path=$dataset_ens_path

startTime=`date +%s`
ts=`date +%Y%m%d.%H%M%S`

rlog=$log_path/mfg_runlog-$ts

self_log=0

ts=`date +%Y%m%d.%H%M%S`
if [ $self_log -eq 1 ]; then
    echo "TS_$ts:STATUS:make_mapfile: processing dataset $dataset_fullpath" >> $rlog 2>&1
else
    echo "TS_$ts:STATUS:make_mapfile: processing dataset $dataset_fullpath"
fi

ds_tm1=`date +%s`

this_user=`whoami`
#conda init bash
anaconda_profile=`$DSM_GETPATH USER_ROOT`/$this_user/anaconda3/etc/profile.d/conda.sh
conda activate pub
esgmapfile make --debug -i $ini_path --max-processes $proc_num --project e3sm --mapfile .mapfile --outdir $out_path $dataset_fullpath
retcode=$?
conda deactivate

ds_tm2=`date +%s`
ds_et=$(($ds_tm2 - $ds_tm1))

ts=`date +%Y%m%d.%H%M%S`
if [ $retcode -eq 0 ]; then
    if [ $self_log -eq 1 ]; then
        echo "TS_$ts:STATUS:COMPLETED: dataset $dataset_fullpath" >> $rlog 2>&1
    else
        echo "TS_$ts:STATUS:COMPLETED: dataset $dataset_fullpath"
    fi
else
    if [ $self_log -eq 1 ]; then
        echo "TS_$ts:STATUS:FAILURE:   dataset $dataset_fullpath (exit code: $retcode)" >> $rlog 2>&1
    else
        echo "TS_$ts:STATUS:FAILURE:   dataset $dataset_fullpath (exit code: $retcode)"
    fi
fi

if [ $self_log -eq 1 ]; then
    echo "TS_$ts:STATUS:dataset ET = $ds_et" >> $rlog 2>&1
else
    echo "TS_$ts:STATUS:dataset ET = $ds_et"
fi

if [ $retcode -ne 0 ]; then
    retcode=1
fi

exit $retcode


