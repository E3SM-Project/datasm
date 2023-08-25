#!/bin/bash

note='Usage: datasm_extract_from_archive.sh <dsid_list_file> [prestage] [archmap=<path_to_archive_map>]'

note='Unlike the previous "arch_extract_am_list.sh", this routine expects a list of dataset_ids.'
note='If extraction_requests_pending/ is not empty, it complains and exits,'
note='It finds all archive_map lines appropriate to the dataset, and places them into a dsid-named request'
note='The new tickets are moved to /p/user_pub/e3sm/archive/.extraction_requests_pending/, unless.'
note='the term "prestage" exists on the commandline, in which case the tickets are placed into'
note='/p/user_pub/e3sm/archive/.extraction_requests_prestage/, allowing the user to copy them into'
note='/p/user_pub/e3sm/archive/.extraction_requests_pending/ in a preferred order.'
note='If no archmap= is specified, the default (/p/user_pub/e3sm/archive/.cfg/Archive_Map) is used.'

note='If the "archive_extraction_service" is not running, it is started.'

user=`whoami`
userpath=/p/user_pub/e3sm/$user

dsid_list=$1

if [[ $# -eq 0 ]]; then
    echo "Usage: datasm_extract_from_archive.sh <dsid_list_file> [prestage] [archmap=<path_to_archive_map>]"
    exit 0
fi


arch_map="/p/user_pub/e3sm/archive/.cfg/Archive_Map"
prestage=0

i=1
while [ $i -le $# ]; do
    if [[ ${!i:0:8} == "archmap=" ]]; then
        arch_map=${!i:8}
        echo ARCH_MAP=$arch_map
    fi
    if [[ ${!i} == "prestage" ]]; then
        prestage=1
    fi
    ((++i))
done


workdir=$userpath/Pub_Work/0_Extraction
extr_reqs_pend="/p/user_pub/e3sm/archive/.extraction_requests_pending"
extr_reqs_pres="/p/user_pub/e3sm/archive/.extraction_requests_prestage"

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

ticket_count=`ls $workdir/tmp_requests/extraction_request-* | wc -l`

if [[ $prestage -eq 1 ]]; then
    mv $workdir/tmp_requests/extraction_request-* $extr_reqs_pres
    echo "$ticket_count new tickets moved to /p/user_pub/e3sm/archive/.extraction_requests_prestage for sort/copy to pending"
else
    mv $workdir/tmp_requests/extraction_request-* $extr_reqs_pend
    echo "$ticket_count new tickets moved to /p/user_pub/e3sm/archive/.extraction_requests_pending"
fi

/p/user_pub/e3sm/staging/tools/restart_services.sh extraction

exit 0

