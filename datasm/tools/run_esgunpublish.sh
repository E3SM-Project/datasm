#!/bin/bash

# Unpublish each dataset in the file-list of dataset_ids provided.
#
# Usage:  run_esgunpublish file_list_of_dataset_ids [DELETE]
#
#       Default operation is "retraction" if DELETE is not specified
#       All versions are retracted or deleted on all known datanodes.
#       Any status file under [STAGING_STATUS] is updated.

dryrun=0

dsidlist=$1
target_op="Retracted"
do_delete=""
if [ $# -eq 2 ]; then
    if [ $2 == "DELETE" ]; then
        do_delete="--delete"
        target_op="Deleted"
    fi
fi

resource_path=`$DSM_GETPATH STAGING_RESOURCE`
dsm_tools=`$DSM_GETPATH STAGING_TOOLS`
pub_root=`$DSM_GETPATH PUBLICATION_DATA`
statpath=`$DSM_GETPATH STAGING_STATUS`

datanodes=`cat $resource_path/publication_datanodes | head -1`
latest_data=$dsm_tools/latest_data_location_by_dsid.sh

ts=`date -u +%Y%m%d_%H%M%S_%6N`

pwdpath=`pwd`
workpath=`realpath $pwdpath`
pub_log=$workpath/UnPublication_Log-$ts
cert_file=$resource_path/certs/publication/certificate-file

# NOTE:  Ensure proper conda environment to issue esgunpublish command

have_esgcet=`conda list | grep esgcet | wc -l`
if [ $have_esgcet -eq 0 ]; then
    env=`conda info | grep "active environment" | cut -f2 -d: | tr -d ' '`
    echo "ERROR: Current environment \"$env\" is missing esgcet"
    exit 0
fi

for dsid in `cat $dsidlist`; do
    # obtain status file for recordkeeping
    statfile="$statpath/${dsid}.status"
    # obtain highest version
    corepath=`echo $dsid | tr . /`
    edir=$pub_root/$corepath
    vdir=`ls $edir | egrep "^[v]" | tail -1`


    full_vdir=`$latest_data $dsid`
    vnum=`basename $full_vdir | cut -c2-`

    ts=`date -u +%Y%m%d_%H%M%S_%6N`
    vdir=v$vnum
    echo "$ts: unpublishing $dsid (version $vdir)" >> $pub_log 2>&1
    inst_id=${dsid}.$vdir
    for dn in $datanodes; do
        ts=`date -u +%Y%m%d_%H%M%S_%6N`
        echo "$ts:cmd = esgunpublish --data-node $dn --dset-id $inst_id $do_delete" >> $pub_log 2>&1
        if [ $dryrun -eq 1 ]; then
            continue
        fi
        esgunpublish --data-node $dn --dset-id $inst_id $do_delete >> $pub_log 2>&1
    done
    if [ $dryrun -eq 1 ]; then
        continue
    fi
    ts=`date -u +%Y%m%d_%H%M%S_%6N`
    echo "STAT:$ts:ESGUNPUBLISH:$target_op" >> $statfile

done

echo "Unpublication Completed"




