#!/bin/bash

# FORMAT
# esgmapfile_make.sh <dataset_fullpath>
# Note: output mapfile fullpath will be <dataset_fullpath>.mapfile

proc_num=30

workpath=/p/user_pub/e3sm/bartoletti1/Pub_Work/2_Mapwork
ini_path=/p/user_pub/e3sm/staging/ini_std/

startTime=`date +%s`
ts=`date +%Y%m%d.%H%M%S`
rlog=$workpath/mfg_runlog-$ts

dataset_ens_path=$1
dataset_fullpath=$2

self_log=0

# Obtain the expected mapfile name, "<dsid>.map"
# must determine if "fullpath" begins with "/p/user_pub/e3sm" (warehouse) or "/p/user_pub/work" (publication)
# this is brittle - should be generalized
home=`echo $dataset_ens_path | cut -f4 -d/`
if [ $home == "e3sm" ]; then
    part=`echo $dataset_ens_path | cut -f7- -d/ | tr / .`
    dsid='E3SM.'${part}
elif [ $home == "work" ]; then
    dsid=`echo $dataset_ens_path | cut -f5- -d/ | tr / .`
else
    if [ $self_log -eq 1 ]; then
        echo "TS_$ts:ERROR:ESGMM: Unexpected dataset path: $dataset_ens_path" >> $rlog 2>&1
    else
        echo "TS_$ts:ERROR:ESGMM: Unexpected dataset path: $dataset_ens_path"
    fi
    exit 1
fi

mapfile_name=mapfile.map
echo "DATASET_ENS_PATH: $dataset_ens_path"
echo "DATASET_FULLPATH: $dataset_fullpath"
echo "MAPFILE_NAME: $mapfile_name"

ts=`date +%Y%m%d.%H%M%S`
if [ $self_log -eq 1 ]; then
    echo "TS_$ts:STATUS:ESGMM: make_mapfile: processing dataset $dataset_fullpath" >> $rlog 2>&1
else
    echo "TS_$ts:STATUS:ESGMM: make_mapfile: processing dataset $dataset_fullpath"
fi

ds_tm1=`date +%s`

#conda init bash
source ~/anaconda3/etc/profile.d/conda.sh
conda activate pub
esgmapfile make --debug -i $ini_path --max-processes $proc_num --project e3sm --mapfile .mapfile --outdir $dataset_ens_path $dataset_fullpath
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


exit $retcode


