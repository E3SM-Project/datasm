#!/bin/bash

dsid_list=$1

dsid_list_name=`basename $dsid_list`

tools=`$DSM_GETPATH STAGING_TOOLS`
latest=$tools/latest_data_location_by_dsid.sh
opdsid=$tools/optional_v3_cmip6_dsid.sh
statfiles=`$DSM_GETPATH STAGING_STATUS`

ts=`date -u +%Y%m%d_%H%M%S_%6N`

log="log-dsm_generate_mapfiles-$ts"

ds_count=`cat $dsid_list | wc -l`

echo "$ts:Begin mapfile generation for dsid_list $dsid_list_name: ($ds_count datasets)" >> $log

uts1=`date -u +%s`

for dsid in `cat $dsid_list`; do
    src_data=`$latest $dsid`
    ds_version=`basename $src_data`
    ds_base_path=`dirname $src_data`
    statfile=$statfiles/${dsid}.status

    # Prepare for v3 data dsid substitution
    eff_dsid=`$opdsid $dsid`

    ts1=`date -u +%Y%m%d_%H%M%S_%6N`
    $tools/create_checksum_manifest_for_dsid.sh $eff_dsid > /dev/null 2>&1
    rc=$?
    ts2=`date -u +%Y%m%d_%H%M%S_%6N`
    if [[ $rc -eq 0 ]]; then
        echo "$ts2:MapfileGeneration:Pass:dsid=$dsid:version=$ds_version" >> $log
        echo "STAT:$ts2:PUBLICATION:MapfileGeneration:Pass:version=$ds_version" >> $statfile
    else
        echo "$ts2:MapfileGeneration:Fail:dsid=$dsid:version=$ds_version" >> $log
        echo "STAT:$ts2:PUBLICATION:MapfileGeneration:Fail:version=$ds_version" >> $statfile
    fi
done

uts2=`date -u +%s`
et=$((uts2 - uts1))

ts2=`date -u +%Y%m%d_%H%M%S_%6N`
echo "$ts2:Completed mapfile generation for dsid_list $dsid_list_name: ($ds_count datasets) ET=$et seconds" >> $log


        
