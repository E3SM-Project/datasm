#!/bin/bash

mapfile_dir=$1
mapfile_done=$2
project=$3
email=$4
ini_dir=/p/user_pub/work/E3SM/ini

mapfiles=( `ls $mapfile_dir` )

for mapfile in "${mapfiles[@]}"
do
    esgpublish -i $ini_dir --project $project --map $mapfile_dir/$mapfile --commit-every 100  --no-thredds-reinit
    if [ $? != 0 ] ; then
        echo Failed to engest $mapfile into the database
        exit 1
    fi
done


for mapfile in "${mapfiles[@]}"
do
    esgpublish -i $ini_dir --project $project --map $mapfile_dir/$mapfile --service fileservice --noscan --thredds  --no-thredds-reinit
    if [ $? != 0 ] ; then
        echo Failed to engest $mapfile into thredds
        exit 1
    fi
done

esgpublish --project $project --thredds-reinit

for mapfile in "${mapfiles[@]}"
do
    esgpublish -i $ini_dir --project $project --map $mapfile_dir/$mapfile --service fileservice --noscan --publish
    if [ $? != 0 ] ; then
        echo Failed to publish $mapfile
        exit 1
    fi
    mv -v $mapfile_dir/$mapfile $mapfile_done/
done

esgpublish --project $project --thredds-reinit

echo "All done" | sendmail $4