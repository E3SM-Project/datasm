#!/bin/bash

utcStart=`date +%s`

mapfile_dir=/p/user_pub/e3sm/staging/mapfiles
mapfile_done=/p/user_pub/e3sm/staging/mapfiles_temp_done
project=e3sm
email_recip=bartoletti1@llnl.gov

ini_dir=/p/user_pub/e3sm/staging/ini_old

exit_on_any_fail=1

mapfile_count=`ls $mapfile_dir | wc -l`
mapfiles_published=0

mapfiles=( `ls $mapfile_dir` )

for mapfile in "${mapfiles[@]}"
do
    esgpublish -i $ini_dir --project $project --map $mapfile_dir/$mapfile --commit-every 100  --no-thredds-reinit
    if [ $? != 0 ] ; then
        echo Failed to engest $mapfile into the database
	if [ $exit_on_any_fail ]; then
            exit 1
        else
            continue
        fi
    fi
    echo Successfully engested $mapfile into the database

    esgpublish -i $ini_dir --project $project --map $mapfile_dir/$mapfile --service fileservice --noscan --thredds  --no-thredds-reinit
    if [ $? != 0 ] ; then
        echo Failed to engest $mapfile into thredds
	if [ $exit_on_any_fail ]; then
            exit 1
        else
            continue
        fi
    fi
    echo Successfully engested $mapfile into thredds

    esgpublish --project $project --thredds-reinit

    esgpublish -i $ini_dir --project $project --map $mapfile_dir/$mapfile --service fileservice --noscan --publish
    if [ $? != 0 ] ; then
        echo Failed to publish $mapfile
	if [ $exit_on_any_fail ]; then
            exit 1
        else
            continue
        fi
    fi
    echo Successfully published $mapfile

    mapfiles_published=$(($mapfiles_published + 1))

    mv -v $mapfile_dir/$mapfile $mapfile_done/

done

esgpublish --project $project --thredds-reinit

utcFinal=`date +%s`
elapsed=$(($utcFinal - $utcStart))

echo "All done.  Published $mapfiles_published of $mapfile_count mapfiles.  Elapsed time:  $elapsed seconds" | sendmail $email_recip

