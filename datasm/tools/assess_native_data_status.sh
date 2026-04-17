#!/bin/bash

# Given a native dataset_id, year-range  and an alternative local path, determine if
#  a. The warehouse has native data for the years given (report WH_READY)
#  b. The local path has native data for the years given (report LOCAL_READY)
# This is designed to support hybrid "maybe local"/"maybe NERSC fetched".
# If in-warehouse complete, returns:
#    READY:<dsid>:<full_warehouse_path>
# Else if local-user complete, returns:
#    READY:<dsid>:<full_local_path>
# Else, return
#    FAIL:<dsid>
# NOTE: If files in local_path are complete, one must still employ
#    <path>/<pattern>
# Where pattern=`grep $nat_dsid $arch_map | cut -f4 -d,`
# because the <path> may contain files for several frequencies, etc. 

nat_dsid=$1
year_range=$2
local_path=$3

tools=`$DSM_GETPATH STAGING_TOOLS`
latest=$tools/latest_data_location_by_dsid.sh

warehouse=`$DSM_GETPATH STAGING_DATA`

# obtain warehouse path

wh_path=`$latest $nat_dsid`

# echo "INFO: WH_PATH = $wh_path"
# echo "INFO: LOCAL_PATH = $local_path"

# setup loop on years

yr1=`echo $year_range | cut -f1 -d-`
yr2=`echo $year_range | cut -f2 -d-`

PrimaryFail=0
SecondaryFail=0

if [[ 1 -eq 0 ]]; then
    if [[ $wh_path != "NONE" ]]; then
        yr=$yr1
        while [[ $yr -le $yr2 ]]; do
            # echo -n "YEAR: $yr"
            yr_files=`ls $wh_path | grep $yr | wc -l`
            # echo " ($yr_files files)"
            if [[ $yr_files -ne 12 ]]; then
                PrimaryFail=1
                break
            fi
            yr=$((yr + 1))
        done

        if [[ $PRIMARY_FAIL -eq 0 ]]; then
            use_path=`dirname $wh_path`
            echo "READY:$nat_dsid:$use_path"
            exit 0
        fi
    fi
fi

# HERE, we need to get the file match pattern from the Archive_Map
# and THEN grep for year

arch_map=`$DSM_GETPATH ARCHIVE_MANAGEMENT`/Archive_Map
filepatt=`grep $nat_dsid $arch_map | cut -f4 -d,`

echo "LOCAL_SEEK = $local_path/$filepatt"

yr=$yr1
while [[ $yr -le $yr2 ]]; do
    # echo -n "YEAR: $yr"
    yr_files=`ls $local_path/$filepatt | grep $yr | wc -l`
    # echo " ($yr_files files)"
    if [[ $yr_files -ne 12 ]]; then
        SecondaryFail=1
        break
    fi
    yr=$((yr + 1))
done

if [[ $Secondary_FAIL -eq 0 ]]; then
    use_path=$local_path
    echo "READY:$nat_dsid:$use_path"
    exit 0
fi

echo "FAIL:$nat_dsid"
