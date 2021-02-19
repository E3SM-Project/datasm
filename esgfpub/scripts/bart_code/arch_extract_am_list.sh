#!/bin/bash

am_list=$1

workdir=/p/user_pub/e3sm/bartoletti1/Pub_Work/0_Extraction

ts=`date +%Y%m%d.%H%M%S`

tmpdir=/tmp/extr_reqs-$ts

mkdir $tmpdir
cd $tmpdir

python $workdir/archive_map_to_dsid.py --input $workdir/$am_list --prefix "extraction_request-"

mv extraction_request-* /p/user_pub/e3sm/archive/.extraction_requests_pending/

exit 0

