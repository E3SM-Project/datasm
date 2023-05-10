#!/bin/bash

note='Usage: datasm_extract_from_archive.sh  dsid_list_file'

note='Unlike the previous "arch_extract_am_list.sh", this routine expects a list of dataset_ids.'
note='If extraction_requests_pending/ is not empty, it complains and exits,'
note='It finds all archive_map lines appropriate to the dataset, and places then into a dsid-named request'
note='The new tickets are moved to /p/user_pub/e3sm/archive/.extraction_requests_pending/.'
note='If the "archive_extraction_service" is not running, it is started.'

user=`whoami`
userpath=/p/user_pub/e3sm/$user

dsid_list=$1

workdir=$userpath/Pub_Work/0_Extraction
extr_reqs_pend="/p/user_pub/e3sm/archive/.extraction_requests_pending"
arch_map="/p/user_pub/e3sm/archive/.cfg/Archive_Map"

# Override manually if needed
do_force=0

if [ $# -eq 2 ]; then
    arg2=$2
    if [ ${arg2:0:3} == "am=" ]; then
        slen=${#arg2}
        arch_map=${arg2:3: $slen}
    fi
fi

extraction_service=/p/user_pub/e3sm/staging/tools/archive_extraction_service.py
mapfilegen_service=/p/user_pub/e3sm/staging/tools/mapfile_generation_service.py

if [ ! -d $userpath ]; then
    echo "ERROR: No user path \"$userpath\" found."
    exit 1
fi

qcount=`ls $extr_reqs_pend | wc -l`
if [ $qcount -gt 0 ]; then
    echo "Error: Request Queue /p/user_pub/e3sm/archive/.extraction_requests_pending is not empty.  Use 'force' to override."
    exit 0
fi

mkdir -p $workdir
mkdir -p $workdir/tmp_requests

# clear out old local tickets
rm -f $workdir/tmp_requests/extraction_request-*

ts=`date -u +%Y%m%d_%H%M%S_%6N`

# create an extraction request ticket for each dataset_id
# (dsid = Proj.Model.Exp.Res.Realm.Grid.OutType.Freq.Ens)

echo "Archive_Extraction: Operating with Archive_Map = $arch_map"

for dsid in `cat $dsid_list`; do

    echo "DSID = $dsid"
    cat $arch_map | fgrep ",${dsid}," > $workdir/tmp_requests/extraction_request-$dsid

    foundsome=`cat $workdir/tmp_requests/extraction_request-$dsid | wc -l`
    if [ $foundsome -eq 0 ]; then
        mv $workdir/tmp_requests/extraction_request-$dsid $workdir/tmp_requests_failed/extraction_request-$dsid
        echo "ERROR: Request Ticket FAILED for key: $key"
    fi

done

mv $workdir/tmp_requests/extraction_request-* $extr_reqs_pend

/p/user_pub/e3sm/staging/tools/restart_services.sh extraction

exit 0

