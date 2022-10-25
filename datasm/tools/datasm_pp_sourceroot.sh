#!/bin/bash

# use parent_native_dsid.sh to obtain the native source type for this CMIP6 dataset.
# use latest_data_location_by_dsid.sh to determine whewre the latest data resides.
# return "warehouse", "publication", or "NONE" accordingly.

cmip6_dsid=$1

parent_dsid=`/p/user_pub/e3sm/staging/tools/parent_native_dsid.sh $cmip6_dsid`

if [ ${parent_dsid:0:4} == "NONE" ]; then
    echo "NONE"
    exit 0
fi

parent_path=`/p/user_pub/e3sm/staging/tools/latest_data_location_by_dsid.sh $parent_dsid`

if [ ${parent_path:0:4} == "NONE" ]; then
    echo "NONE"
    exit 0
fi

# seek one of
# /p/user_pub/work/
# /p/user_pub/e3sm/warehouse/

if [ ${parent_path:0:17} == "/p/user_pub/work/" ]; then
    echo "publication"
    exit 0
fi

if [ ${parent_path:0:27} == "/p/user_pub/e3sm/warehouse/" ]; then
    echo "warehouse"
    exit 0
fi

echo "NONE"

exit 0



