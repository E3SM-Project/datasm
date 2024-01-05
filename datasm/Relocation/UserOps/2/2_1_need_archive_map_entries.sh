#!/bin/bash

dsidlist=$1

tools=`$DSM_GETPATH STAGING_TOOLS`

dsid_to_amlines=$tools/dsids_to_archive_map_lines.sh

$dsid_to_amlines $dsidlist | grep NONE | cut -f1 -d:


