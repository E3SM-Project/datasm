#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Usage: update_latest_files_for_metadata_references_by_dsid_list.sh <file_of_dataset_ids> <file_or_reference_text>"
    exit 0
fi

dsidlist=$1
ref_text=`cat $2`

dsm_tools=`$DSM_GETPATH STAGING_TOOLS`
dspi=$dsm_tools/ds_paths_info.sh
latest=$dsm_tools/latest_data_location_by_dsid.sh


ts=`date -u +%Y%m%d_%H%M%S_%6N`
echo "Start: the_ts=$ts"

tot_fccount=0

for dsid in `cat $dsidlist`; do
    fcount=0
    statfile=`$dspi $dsid | grep SF_PATH | cut -f2 -d' '`
    fullpath=`$latest $dsid`
    if [ $fullpath == "None" ]; then
        echo "ERROR: $dsid: No valid data source path found"
        continue
    fi
    thecount=`ls $fullpath | wc -l`
    echo $fullpath: $thecount

    for afile in `ls $fullpath`; do
        ncatted --glb_att_add references=\\"$ref_text\\" --hst $fullpath/$afile
        fcount=$((fcount + 1))
    done
    ts=`date -u +%Y%m%d_%H%M%S_%6N`
    echo "STAT:$ts:TOOLS:MetadataUpdate:References" >> $statfile
    echo "STAT:$ts:PUBLICATION:Metadata:Cleared" >> $statfile
    echo "DSID:$dsid Updated References in $fcount datafiles"
    tot_fcount=$((tot_fcount + fcount))
done

echo " "
echo "Total File Count: $tot_fcount"

ts=`date -u +%Y%m%d_%H%M%S_%6N`
echo "Final: the_ts=$ts"

exit 0
    
