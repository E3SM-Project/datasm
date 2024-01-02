#!/bin/bash

# dsids generated by dataset_spec.yaml, may or may not have been found in archive_mapping
in_dsids=$1


for dsid in `cat $in_dsids`; do grep $dsid /p/user_pub/e3sm/staging/resource/archive/Archive_Map | cut -f2 -d,; done 
