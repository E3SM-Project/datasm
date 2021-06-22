#!/bin/bash

note='turns a list of archive_map lines into individual extraction requests for the extraction service'
note='user must move these to: /p/user_pub/e3sm/archive/.extraction_requests_pending/'

am_list=$1

workdir=/p/user_pub/e3sm/bartoletti1/Pub_Work/0_Extraction

ts=`date +%Y%m%d.%H%M%S`

tmpdir=/tmp/extr_reqs-$ts

mkdir $tmpdir
cd $tmpdir

python $workdir/archive_map_to_dsid.py --input $workdir/$am_list --prefix "extraction_request-"

# TEMP_COMMENT mv extraction_request-* /p/user_pub/e3sm/archive/.extraction_requests_pending/
mv extraction_request-* $workdir


exit 0

