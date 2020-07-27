#!/bin/bash

if [ $# -ne 1 ]; then
	echo "Please enter a descriptive job_name for tracking purposes"
	exit 1
fi

tag=`echo $1 | tr ' ' _`


utcStart=`date +%s`
ts=`date +%Y%m%d.%H%M%S`

rlog="rlog-$ts-$tag"

logdir=/p/user_pub/e3sm/bartoletti1/Pub_Work/3_Publish/runlogs
mapfile_indir=/p/user_pub/e3sm/staging/mapfiles/stage_3_publish
mapfile_exdir=/p/user_pub/e3sm/staging/mapfiles/mapfiles_archive/BGC/mapfiles_bgc_raw
ini_dir=/p/user_pub/e3sm/staging/ini_std
project=e3sm
email_recip=bartoletti1@llnl.gov


mapfiles=( `ls $mapfile_indir` )
mapfile_count=${#mapfiles[@]}

ts=`date +%Y%m%d.%H%M%S`
echo "$ts: INPROCESS: calling --thredds-reinit ($mapfile_count mapfiles)" > $logdir/$rlog 2>&1

esgpublish --project $project --thredds-reinit >> $logdir/$rlog 2>&1

ts=`date +%Y%m%d.%H%M%S`
echo "$ts: COMPLETED: thredds-reinit ($mapfile_count mapfiles)" >> $logdir/$rlog 2>&1
echo "$ts: INPROCESS: mapfile publication ($mapfile_count mapfiles)" >> $logdir/$rlog 2>&1
for mapfile in "${mapfiles[@]}"
do
    esgpublish -i $ini_dir --project $project --map $mapfile_indir/$mapfile --service fileservice --noscan --publish >> $logdir/$rlog 2>&1
    if [ $? != 0 ] ; then
        echo Failed to publish $mapfile >> $logdir/$rlog 2>&1
        exit 1
    fi
    echo Successfully published $mapfile >> $logdir/$rlog 2>&1
    mv -v $mapfile_indir/$mapfile $mapfile_exdir/
done

utcFinal=`date +%s`
elapsed=$(($utcFinal - $utcStart))

ts=`date +%Y%m%d.%H%M%S`
echo "$ts: COMPLETED: mapfile publication ($mapfile_count mapfiles)" >> $logdir/$rlog 2>&1
echo "$ts: Elapsed time:  $elapsed seconds" >> $logdir/$rlog 2>&1

echo "All done ($tag).  Elapsed time:  $elapsed seconds" | sendmail $email_recip

