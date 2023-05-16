#!/bin/bash

# must supply a CMIP6 dataset_id
in_dsid=$1

parent=/p/user_pub/e3sm/staging/tools/parent_native_dsid.sh
latest=/p/user_pub/e3sm/staging/tools/latest_data_location_by_dsid.sh
derivc=/p/user_pub/e3sm/staging/tools/derivative_conf.sh

parent_dsid=`$parent $in_dsid`
input_data=`$latest $parent_dsid`

headpart=`echo $parent_dsid | cut -f1-6 -d.`
tailpart=`echo $parent_dsid | cut -f9 -d.`

namefile_dsid=${headpart}.namefile.fixed.${tailpart}
restart_dsid=${headpart}.restart.fixed.${tailpart}

namefile_data_path=`$latest $namefile_dsid`
restart_data_path=`$latest $restart_dsid`

namefile=`ls $namefile_data_path | head -1`
restartf=`ls $restart_data_path | head -1`

namefile_data=$namefile_data_path/$namefile
restart_data=$restart_data_path/$restartf


echo "input_data:$input_data"
echo "namefile_data:$namefile_data"
echo "restart_data:$restart_data"
$derivc $in_dsid fullpath


