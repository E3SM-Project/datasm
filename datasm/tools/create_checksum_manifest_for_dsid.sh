#!/bin/bash

# The format of mapfile entries is:

# <dsid>#<versiondate> | <local_data_vpath>/<filename> | <filesize> | mod_time=<time> | checksum=<checksum> | checksum_type=<checksum_type>
 
# CMIP6.CMIP.E3SM-Project.E3SM-2-1.1pctCO2.r1i1p1f1.AERmon.abs550aer.gr#20240206 | /lcrc/group/e3sm2/DSM/Staging/Data/CMIP6/CMIP/E3SM-Project/E3SM-2-1/1pctCO2/r1i1p1f1/AERmon/abs550aer/gr/v20240206/abs550aer_AERmon_E3SM-2-1_1pctCO2_r1i1p1f1_gr_005101-010012.nc | 124552780 | mod_time=1723090879.0095928 | checksum=aafab7cc04ed90c9b97df006ee54606a8ac498a5973b6f08944bf7cae287779c | checksum_type=SHA256

# mapfile is generated locally as <dsid>.map, but moved to STAGING_DATA/<path_to_dsid_versions>/.mapfile-<ds_version>.map

dsid=$1
# Configure and call datasm/scripts/generate_mapfile.py

tools=`$DSM_GETPATH STAGING_TOOLS`
latest=$tools/latest_data_location_by_dsid.sh
mapgen=$tools/dsm_generate_mapfile.py

src_data=`$latest $dsid`

if [[ ! -d $src_data ]]; then
    echo "Error: Failed to locate source data for $dsid"
    exit 1
fi

ds_version=`basename $src_data`
ds_verdate=`basename $src_data | cut -c2-`
ds_basepath=`dirname $src_data`

python $mapgen --quiet $src_data $dsid $ds_verdate

mapfile="${dsid}.map"

if [[ ! -f $mapfile ]]; then
    echo "Error: Failed to produce mapfile for $dsid"
    exit 1
fi

mv $mapfile $ds_basepath/.mapfile-${ds_version}.map

exit 0

