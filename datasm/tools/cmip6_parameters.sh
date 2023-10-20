#!/bin/bash

# must supply a CMIP6 dataset_id
in_dsid=$1

tools=`$DSM_GETPATH STAGING_TOOLS`
parent=$tools/parent_native_dsid.sh
latest=$tools/latest_data_location_by_dsid.sh
derivc=$tools/derivative_conf.sh

parent_dsid=`$parent $in_dsid`
input_data=`$latest $parent_dsid`

headpart=`echo $parent_dsid | cut -f1-6 -d.`
tailpart=`echo $parent_dsid | cut -f9 -d.`

namefile_dsid=${headpart}.namefile.fixed.${tailpart}
restart_dsid=${headpart}.restart.fixed.${tailpart}

namefile_data_path=`$latest $namefile_dsid`
restart_data_path=`$latest $restart_dsid`
restart_data_path=${restart_data_path/"sea-ice"/"ocean"}

if [[ $namefile_data_path == "NONE" ]]; then
    namefile_data="NONE"
else
    namefile=`ls $namefile_data_path | head -1`
    namefile_data=$namefile_data_path/$namefile
fi
if [[ $restart_data_path == "NONE" ]]; then
    restart_data="NONE"
else
    restartf=`ls $restart_data_path | head -1`
    restart_data=$restart_data_path/$restartf
fi


echo "parent_native_dsid:$parent_dsid"
echo "input_data:$input_data"
echo "namefile_data:$namefile_data"
echo "restart_data:$restart_data"
$derivc $in_dsid fullpath



