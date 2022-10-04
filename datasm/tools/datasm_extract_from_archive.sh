#!/bin/bash

note='Usage: datasm_extract_from_archive.sh  dsid_list_file'

note='Unlike the previous "arch_extract_am_list.sh", this routine expects a list of dataset_ids.'
note='If extraction_requests_pending/ is not empty, it complains and exits unless "--force" is supplied'
note='It finds all archive_map lines appropriate to the dataset, and places then into a dsid-named request'
note='The new tickets are moved to /p/user_pub/e3sm/archive/.extraction_requests_pending/.'
note='If the "archive_extraction_service" is not running, it is started.'

dsid_list=$1

do_force=0
if [ $# -eq 2 ]; then
    if [ $2 == "force" ]; then
        do_force=1
    fi
fi

user=`whoami`
userpath=/p/user_pub/e3sm/$user

extraction_service=/p/user_pub/e3sm/staging/tools/archive_extraction_service.py
mapfilegen_service=/p/user_pub/e3sm/staging/tools/mapfile_generation_service.py

if [ ! -d $userpath ]; then
    echo "ERROR: No user path \"$userpath\" found."
    exit 1
fi


workdir=$userpath/Pub_Work/0_Extraction
extr_reqs_pend="/p/user_pub/e3sm/archive/.extraction_requests_pending"
arch_map="/p/user_pub/e3sm/archive/.cfg/Archive_Map"

if [ $do_force -eq 0 ]; then
    qcount=`ls $extr_reqs_pend | wc -l`
    if [ $qcount -gt 0 ]; then
        echo "Error: Request Queue /p/user_pub/e3sm/archive/.extraction_requests_pending is not empty.  Use 'force' to override."
        exit 0
    fi
fi

mkdir -p $workdir
mkdir -p $workdir/tmp_requests

# clear out old local tickets
rm -f $workdir/tmp_requests/extraction_request-*

ts=`date -u +%Y%m%d_%H%M%S_%6N`

# create an extraction request ticket for each dataset_id
# (dsid = Proj.Model.Exp.Res.Realm.Grid.OutType.Freq.Ens)

for dsid in `cat $dsid_list`; do
    pat_1=`echo $dsid | cut -f2-4 -d. | tr . ,`
    realm=`echo $dsid | cut -f5 -d.`
    grid=`echo $dsid | cut -f6 -d.`
    otyp=`echo $dsid | cut -f7 -d.`
    freq=`echo $dsid | cut -f8 -d.`
    ensm=`echo $dsid | cut -f9 -d.`
    
    if [ $grid == "native" ]; then
        grid="nat"
    fi
    if [ $realm == "atmos" ]; then
        realm="atm"
    elif [ $realm == "land" ]; then
        realm="lnd"
    elif [ $realm == "ocean" ]; then
        realm="ocn"
    fi

    dtype="${realm}_${grid}_${freq}"
    key="$pat_1,$ensm,$dtype,$otyp"

    # echo "KEY = $key"
    cat $arch_map | grep $key > $workdir/tmp_requests/extraction_request-$dsid

done

mv $workdir/tmp_requests/extraction_request-* $extr_reqs_pend

is_running=`ps aux | grep "python archive_extraction_service" | grep -v grep | wc -l`

if [ $is_running -eq 0 ]; then
    /p/user_pub/e3sm/staging/tools/restart_services.sh extraction
fi


exit 0

