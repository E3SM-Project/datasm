#!/bin/bash

dsidlist=$1

tools=`$DSM_GETPATH STAGING_TOOLS`

get_years=$tools/tell_years_dsid.py
parent_id=$tools/parent_native_dsid.sh
latestdir=$tools/latest_data_location_by_dsid.sh

ts=`date -u +%Y%m%d_%H%M%S_%6N`
excess_list=trim_extraction/excess_list-$ts-$dsidlist

for in_dsid in `cat $dsidlist`; do

    dsid=`$parent_id $in_dsid`
    start_end=`python $get_years -d $dsid`

    yr_start=`echo $start_end | cut -f1 -d,`
    yr_final=`echo $start_end | cut -f2 -d,`

    # echo "$dsid: $yr_start $yr_final"

    ds_dir=`$latestdir $dsid`
    # echo "ds_dir: $ds_dir"

    for afile in `ls $ds_dir`; do

        if [[ $afile =~ -*([0-9]{4}-[0-9]{2}) ]]; then
            year_mo=${BASH_REMATCH[1]}
            year=${BASH_REMATCH[1]:0:4}
            year=$((10#$year))
            # echo "FILE YEAR: $year"
            if [[ $year -lt $yr_start ]]; then
                echo "EXCESS:$ds_dir/$afile" >> $excess_list
            fi
            if [[ $year -gt $yr_final ]]; then
                echo "EXCESS:$ds_dir/$afile" >> $excess_list
            fi
        fi

    done

done

echo "COMPLETED trim_report.  Please review $excess_list before proceeding with remove_excess."


