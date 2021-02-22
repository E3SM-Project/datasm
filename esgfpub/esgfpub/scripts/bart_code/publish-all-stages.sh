#!/bin/bash

if [ $# -ne 1 ]; then
        echo "Please enter a descriptive job_name for tracking purposes"
        exit 1
fi

tagname=`echo $1 | tr ' ' _`


utcStart=`date +%s`
ts=`date +%Y%m%d.%H%M%S`

rlog="rlog-$ts-$tagname"

logdir=/p/user_pub/e3sm/bartoletti1/Pub_Work/3_Publish/runlogs
mapfiles_stage1=/p/user_pub/e3sm/staging/mapfiles/stage_1_db_ingest
mapfiles_stage2=/p/user_pub/e3sm/staging/mapfiles/stage_2_thredds_ingest
mapfiles_stage3=/p/user_pub/e3sm/staging/mapfiles/stage_3_publish
mapfiles_aborted=/p/user_pub/e3sm/staging/mapfiles/mapfiles_hold
mapfiles_archive=/p/user_pub/e3sm/staging/mapfiles/mapfiles_archive
ini_dir=/p/user_pub/e3sm/staging/ini_std
project=e3sm
email_recip=bartoletti1@llnl.gov

mapfiles=( `ls $mapfiles_stage1 | grep -v .part` )
mapfile_count=${#mapfiles[@]}

ts=`date +%Y%m%d.%H%M%S`
echo "$ts: INPROCESS: mapfile ingestion to database ($mapfile_count mapfiles)" >> $logdir/$rlog 2>&1
for mapfile in "${mapfiles[@]}"
do
    esgpublish -i $ini_dir --project $project --map $mapfiles_stage1/$mapfile --replace --commit-every 100  --no-thredds-reinit >> $logdir/$rlog 2>&1
    retcode=$?
    ts=`date +%Y%m%d.%H%M%S`
    if [ $retcode != 0 ] ; then
        echo "$ts: -- Failed to ingest $mapfile into the database" >> $logdir/$rlog 2>&1
	mv $mapfiles_stage1/$mapfile $mapfiles_aborted/$mapfile
        continue
    fi
    echo "$ts: -- Successfully ingested $mapfile into the database" >> $logdir/$rlog 2>&1
    mv $mapfiles_stage1/$mapfile $mapfiles_stage2/$mapfile
done

ts=`date +%Y%m%d.%H%M%S`
echo "$ts: COMPLETED: mapfile ingestion to database ($mapfile_count mapfiles)" >> $logdir/$rlog 2>&1

mapfiles=( `ls $mapfiles_stage2` )
mapfile_count=${#mapfiles[@]}

ts=`date +%Y%m%d.%H%M%S`
echo "$ts: INPROCESS: mapfile ingestion to thredds ($mapfile_count mapfiles)" >> $logdir/$rlog 2>&1
for mapfile in "${mapfiles[@]}"
do
    esgpublish -i $ini_dir --project $project --map $mapfiles_stage2/$mapfile --service fileservice --noscan --thredds  --no-thredds-reinit >> $logdir/$rlog 2>&1
    retcode=$?
    ts=`date +%Y%m%d.%H%M%S`
    if [ $retcode != 0 ] ; then
        echo "$ts: -- Failed to ingest $mapfile into thredds" >> $logdir/$rlog 2>&1
	mv $mapfiles_stage2/$mapfile $mapfiles_aborted/$mapfile
	continue
    fi
    echo "$ts: -- Successfully ingested $mapfile into thredds" >> $logdir/$rlog 2>&1
    mv $mapfiles_stage2/$mapfile $mapfiles_stage3/$mapfile
done

ts=`date +%Y%m%d.%H%M%S`
echo "$ts: COMPLETED: mapfile ingestion to thredds ($mapfile_count mapfiles)" >> $logdir/$rlog 2>&1

ts=`date +%Y%m%d.%H%M%S`
echo "$ts: INPROCESS: calling --thredds-reinit ($mapfile_count mapfiles)" >> $logdir/$rlog 2>&1

esgpublish --project $project --thredds-reinit >> $logdir/$rlog 2>&1

ts=`date +%Y%m%d.%H%M%S`
echo "$ts: COMPLETED: thredds-reinit ($mapfile_count mapfiles)" >> $logdir/$rlog 2>&1

mapfiles=( `ls $mapfiles_stage3` )
mapfile_count=${#mapfiles[@]}

echo "$ts: INPROCESS: mapfile publication ($mapfile_count mapfiles)" >> $logdir/$rlog
for mapfile in "${mapfiles[@]}"
do
    esgpublish -i $ini_dir --project $project --map $mapfiles_stage3/$mapfile --service fileservice --noscan --publish >> $logdir/$rlog 2>&1
    retcode=$?
    ts=`date +%Y%m%d.%H%M%S`
    if [ $retcode != 0 ] ; then
        echo "$ts: -- Failed to publish $mapfile" >> $logdir/$rlog 2>&1
	mv $mapfiles_stage3/$mapfile $mapfiles_aborted/$mapfile
        continue
    fi
    echo Successfully published $mapfile >> $logdir/$rlog
    mv $mapfiles_stage3/$mapfile $mapfiles_archive/$mapfile
done



utcFinal=`date +%s`
elapsed=$(($utcFinal - $utcStart))

ts=`date +%Y%m%d.%H%M%S`
echo "$ts: COMPLETED: mapfile publication ($mapfile_count mapfiles)" >> $logdir/$rlog 2>&1
echo "$ts: Elapsed time:  $elapsed seconds" >> $logdir/$rlog 2>&1

echo "All done ($tagname).  Elapsed time:  $elapsed seconds" | sendmail $email_recip

