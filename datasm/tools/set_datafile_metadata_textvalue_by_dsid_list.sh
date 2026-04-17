#!/bin/bash

if [ $# -ne 3 ]; then
    echo "Usage: $0 <dsid_list> <attribute_name> <attribute_value>"
    exit 1
fi

dsidlist=$1
att_name=$2
att_text=$3

tools=`$DSM_GETPATH STAGING_TOOLS`
dspi=$tools/ds_paths_info.sh
set_file_metadata_text=$tools/set_datafile_metadata_textvalue.sh

ts=`date -u +%Y%m%d_%H%M%S`
logfile=log-update_$att_name-$ts
echo "Setting $att_name to $att_text for dsidlist $dsidlist" >> $logfile 2>&1

tot_fcount=0
dcount=0

for dsid in `cat $dsidlist`; do
    statfile=`$dspi $dsid | grep SF_PATH | cut -f2 -d' '`
    corepath=`$dspi $dsid | grep WH_PATH | cut -f2 -d' '`
    vdir=`ls $corepath | tail -1`
    # echo $corepath/$vdir
    fcount=0
    for afile in `ls $corepath/$vdir`; do
        $set_file_metadata_text $corepath/$vdir/$afile $att_name \\"$att_text\\" 2>>$logfile
        fcount=$((fcount + 1))
    done
    ts=`date -u +%Y%m%d_%H%M%S_%6N`

    echo "Processed: $dsid: $fcount files" >> $logfile
    echo "STAT:$ts:TOOLS:MetadataUpdate:$att_name" >> $statfile
    tot_fcount=$((tot_fcount + fcount))
    dcount=$((dcount + 1))

done

echo "Completed $dcount datasets ($tot_fcount files)" >> $logfile

exit 0
    
