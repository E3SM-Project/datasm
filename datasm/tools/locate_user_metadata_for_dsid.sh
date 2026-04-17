#!/bin/bash

dsid=$1

resource=`$DSM_GETPATH STAGING_RESOURCE`

IFS='.' read -r -a dsd <<< "$dsid"
sourc=${dsd[3]}
exper=${dsd[4]}
label=${dsd[5]}

metadata_name="${exper}_${label}.json"
metadata_src="$resource/CMIP6-Metadata/$sourc/$metadata_name"

echo $metadata_src

exit 0
