#!/bin/bash

dsidlist=$1

# NOTE:  Only good for ocean at present - need to generalize.

find_latest_path=`$DSM_GETPATH STAGING_TOOLS`/latest_data_location_by_dsid.sh

for dsid in `cat $dsidlist`; do
    latest_path=`$find_latest_path $dsid`

    # file names are all structured as "mpaso.hist.am.timeSeriesStatsMonthly.YYYY-MM-DD.nc"
    first_file=`ls $latest_path | head -1`
    final_file=`ls $latest_path | tail -1`

    echo ""
    echo "DSID: $dsid: first_file=$first_file"
    echo "DSID: $dsid: final_file=$final_file"
    echo ""
    year_list=`ls $latest_path | cut -f5 -d. | cut -f1 -d- | sort | uniq`

    complete=1

    for ayear in $year_list; do
        monthcount=`ls $latest_path | grep "Monthly.$ayear" | wc -l`
        if [ $monthcount -eq 12 ]; then
            continue
        fi
        complete=0
        echo "DSID: $dsid: year $ayear has only $monthcount months."
        for mval in 01 02 03 04 05 06 07 08 09 10 11 12; do
            foundit=`ls $latest_path | grep "Monthly.${ayear}-$mval" | wc -l`
            if [ $foundit -lt 1 ]; then
                echo "DSID: $dsid: MISSING_FILE: mpaso.hist.am.timeSeriesStatsMonthly.${ayear}-${mval}-01.nc"
            fi
        done
    done
    if [ $complete -eq 0 ]; then
        echo "INCOMPLETE_DATASET: $dsid"
    fi
done

