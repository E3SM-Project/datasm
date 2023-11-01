#!/bin/bash

# NOTE: this functionality should match that of the function "derivative_conf()" in datasm/util.py

in_dsid=$1

fullpath=0
if [ $# -eq 2 ]; then
    if [ $2 == "fullpath" ]; then
        fullpath=1
    fi
fi

dsm_tools=`$DSM_GETPATH STAGING_TOOLS`
dsm_resource=`$DSM_GETPATH STAGING_RESOURCE`

get_parent=$dsm_tools/parent_native_dsid.sh
deriv_conf=$dsm_resource/derivatives.conf

# obtain     'hrz_atm_map_path', 'mapfile', 'region_file', 'file_pattern', and 'case_finder'

# 1.  Must obtain "realm,resolution/modelversion" from parent native dataset_id

e3sm_dsid=`$get_parent $in_dsid`

if [ ${e3sm_dsid:0:4} == 'NONE' ]; then
    echo "ERROR: cannot obtain parent e3sm dataset_id for input: $in_dsid"
    exit 1
fi

# E3SM.${modelversion}.${experiment}.${resolution}.${realm}

model=`echo $e3sm_dsid | cut -f2 -d.`
resol=`echo $e3sm_dsid | cut -f4 -d.`
realm=`echo $e3sm_dsid | cut -f5 -d.`

selspec="$realm,$resol,$model"

spec_1="$selspec,REGRID"
spec_2="$selspec,MASK"
spec_3="$selspec,FILE_SELECTOR"
spec_4="$selspec,CASE_FINDER"

regrid=`cat $deriv_conf | grep $spec_1`
if [ `echo $regrid | wc -w` -ne 1 ]; then
    regrid="NONE"
else
    regrid=`echo $regrid | cut -f5 -d,`
fi
region_mask=`cat $deriv_conf | grep $spec_2`
if [ `echo $region_mask | wc -w` -ne 1 ]; then
    region_mask="NONE"
else
    region_mask=`echo $region_mask | cut -f5 -d,`
fi
file_selector=`cat $deriv_conf | grep $spec_3`
if [ `echo $file_selector | wc -w` -ne 1 ]; then
    file_selector="NONE"
else
    file_selector=`echo $file_selector | cut -f5 -d,`
fi
case_finder=`cat $deriv_conf | grep $spec_4`
if [ `echo $case_finder | wc -w` -ne 1 ]; then
    case_finder="NONE"
else
    case_finder=`echo $case_finder | cut -f5 -d,`
fi

mapspath=$dsm_resource/maps

if [ $fullpath -eq 1 ]; then
    echo "hrz_atm_map_path:$mapspath/$regrid"
    echo "mapfile:$mapspath/$regrid"
    if [ $region_mask == "NONE" ]; then
        echo "region_file:NONE"
    else
        echo "region_file:$mapspath/$region_mask"
    fi
else
    echo "hrz_atm_map_path:$regrid"
    echo "mapfile:$regrid"
    if [ $region_mask == "NONE" ]; then
        echo "region_file:NONE"
    else
        echo "region_file:$region_mask"
    fi
fi
echo "file_pattern:$file_selector"
echo "case_finder:$case_finder"

exit 0

