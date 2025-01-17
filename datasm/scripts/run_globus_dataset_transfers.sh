#!/bin/bash

dsidlist=$1

dryrun=0

dsm_tools=`$DSM_GETPATH STAGING_TOOLS`
latest_vpath=$dsm_tools/latest_data_location_by_dsid.sh
staging_data=`$DSM_GETPATH STAGING_DATA`
staging_stat=`$DSM_GETPATH STAGING_STATUS`

NOTE="ENDPOINT DEFINITIONS SECTION" #==========================================================

ACME1_GCSv5_UUID=6edb802e-2083-47f7-8f1c-20950841e46a
ACME1_PREFIX=$staging_data
ACME1_PREFIX_LEN=${#ACME1_PREFIX}

LCRC_IMPROV_DTN_UUID=15288284-7006-4041-ba1a-6b52501e49f1
LCRC_PREFIX="/lcrc/group/e3sm2/DSM/Staging/Data"
LCRC_PREFIX_LEN=${#LCRC_PREFIX}

ALCF_ESGF_NODE_UUID=8896f38e-68d1-4708-bce4-b1b3a3405809
ALCF_PREFIX="/css03_data"
ALCF_PREFIX_LEN=${#ALCF_PREFIX}

NOTE="TRANSFER DEFINITION SECTION" #===========================================================
TASK_TITLE="Globus_Transfer_ACME1_to_LCRC"
SRC_UUID=$ACME1_GCSv5_UUID
SRC_PREFIX=$ACME1_PREFIX
PREFIX_LEN=$ACME1_PREFIX_LEN
DST_UUID=$LCRC_IMPROV_DTN_UUID
DST_PREFIX=$LCRC_PREFIX

ts=`date -u +%Y%m%d_%H%M%S_%6N`
gt_log="globus_transfer_log-$ts"

dsid_count=0

for dsid in `cat $dsidlist`; do
    dsid_count=$((dsid_count + 1))
    if (( $dsid_count % 10 == 0 )); then
        sleep 30
    fi
    lastpath=`$latest_vpath $dsid`
    corepath=${lastpath:$PREFIX_LEN}
    ts=`date -u +%Y%m%d_%H%M%S_%6N`
    dsid_stat_msg_1="$ts:$dsid"
    echo "$ts:globus transfer ${SRC_UUID}:${SRC_PREFIX}/$corepath ${DST_UUID}:${DST_PREFIX}/$corepath" >> $gt_log

    if [[ $dryrun -eq 1 ]]; then
        continue
    fi

    globus transfer ${SRC_UUID}:${SRC_PREFIX}/$corepath ${DST_UUID}:${DST_PREFIX}/$corepath >> $gt_log 2>&1
    rc=$?
    ts=`date -u +%Y%m%d_%H%M%S_%6N`
    if [[ $rc -eq 0 ]]; then
        tr_stat="PASS"
    else
        tr_stat="FAIL=$rc"
    fi
    dsid_stat_msg_2="$ts:PUBLICATION:$TASK_TITLE:$tr_stat"
    echo "$dsid_stat_msg_1:$dsid_stat_msg_2" >> $gt_log
    echo "" >> $gt_log
    statfile=$staging_stat/${dsid}.status
    echo "STAT:$dsid_stat_msg_2" >> $statfile
done


