#!/bin/bash

# FORMAT
# esgmapfile_make.sh <dataset_fullpath>
# Note: output mapfile fullpath will be <dataset_fullpath>.mapfile

ini_path=/p/user_pub/e3sm/staging/ini_std/      #hard-coded for now
log_path=./

proc_num=$1
dataset_fullpath=$2
dataset_ens_path=`echo $dataset_fullpath | rev | cut -f2- -d/ | rev`
out_path=$dataset_ens_path/.mapfile

startTime=`date +%s`
ts=`date +%Y%m%d.%H%M%S`

# where will this get written?  Into the scripts directory?
# not id self_log = 0.
rlog=$log_path/mfg_runlog-$ts

self_log=0

ts=`date +%Y%m%d.%H%M%S`
if [ $self_log -eq 1 ]; then
    echo "TS_$ts:STATUS:ESGMM: make_mapfile: processing dataset $dataset_fullpath" >> $rlog 2>&1
else
    echo "TS_$ts:STATUS:ESGMM: make_mapfile: processing dataset $dataset_fullpath"
fi

ds_tm1=`date +%s`

#conda init bash
source /p/user_pub/e3sm/bartoletti1/anaconda3/etc/profile.d/conda.sh    # HARDCODED!
conda activate pub
esgmapfile make --debug -i $ini_path --max-processes $proc_num --project e3sm --outdir $out_path $dataset_fullpath
retcode=$?
conda deactivate


ds_tm2=`date +%s`
ds_et=$(($ds_tm2 - $ds_tm1))

ts=`date +%Y%m%d.%H%M%S`
if [ $retcode -eq 0 ]; then
    if [ $self_log -eq 1 ]; then
        echo "TS_$ts:STATUS:ESGMM: COMPLETED: dataset $dataset_fullpath" >> $rlog 2>&1
    else
        echo "TS_$ts:STATUS:ESGMM: COMPLETED: dataset $dataset_fullpath"
    fi
else
    if [ $self_log -eq 1 ]; then
        echo "TS_$ts:STATUS:ESGMM: FAILURE:   dataset $dataset_fullpath (exit code: $retcode)" >> $rlog 2>&1
    else
        echo "TS_$ts:STATUS:ESGMM: FAILURE:   dataset $dataset_fullpath (exit code: $retcode)"
    fi
fi

if [ $self_log -eq 1 ]; then
    echo "TS_$ts:STATUS:ESGMM: dataset ET = $ds_et" >> $rlog 2>&1
else
    echo "TS_$ts:STATUS:ESGMM: dataset ET = $ds_et"
fi

if [ $retcode -ne 0 ]; then
    retcode=1
fi

exit $retcode


