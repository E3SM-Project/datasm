#!/bin/bash

# use parent_native_dsid.sh to obtain the native source type for this CMIP6 dataset.
# use latest_data_location_by_dsid.sh to determine whewre the latest data resides.
# return "warehouse", "publication", or "NONE" accordingly.

cmip6_dsid=$1

tools=`$DSM_GETPATH STAGING_TOOLS`
wh_root=`$DSM_GETPATH STAGING_DATA`
pb_root=`$DSM_GETPATH PUBLICATION_DATA`

parent_dsid=`$tools/parent_native_dsid.sh $cmip6_dsid`

if [ ${parent_dsid:0:4} == "NONE" ]; then
    echo "NONE"
    exit 0
fi

parent_path=`$tools/latest_data_location_by_dsid.sh $parent_dsid`

if [ ${parent_path:0:4} == "NONE" ]; then
    echo "NONE"
    exit 0
fi

# seek in STAGING_DATA or PUBLICATION_DATA

if [ ${parent_path:0:16} == $pb_root ]; then
    echo "publication"
    exit 0
fi

if [ ${parent_path:0:26} == $wh_root ]; then
    echo "warehouse"
    exit 0
fi

echo "NONE"

exit 0



