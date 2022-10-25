#!/bin/bash

# won't start with warehouse_dev conda env.

user=`whoami`
userpath=/p/user_pub/e3sm/$user

extraction_service=/p/user_pub/e3sm/staging/tools/archive_extraction_service.py
mapfilegen_service=/p/user_pub/e3sm/staging/tools/mapfile_generation_service.py

if [ ! -d $userpath ]; then
    echo "ERROR: No user path \"$userpath\" found."
    exit 1
fi

if [ $# -ne 1 ]; then
    echo "Error:  must specify extraction or mapfilegen"
    exit 0
fi

ts=`date -u +%Y%m%d_%H%M%S_%6N`

if [ $1 == "extraction" ]; then

    mkdir -p $userpath/Pub_Work/0_Extraction
    cd $userpath/Pub_Work/0_Extraction
    # clean up logs, then
    if [ -f nohup.out ]; then
        mv nohup.out runlogs/nohup.out-$ts
    fi
    nohup python $extraction_service &

    exit 0
fi

if [ $1 == "mapfilegen" ]; then

    cd $userpath/Pub_Work/2_Mapwork
    # clean up logs, then
    if [ -f nohup.out ]; then
        mv nohup.out Runlogs/nohup.out-$ts
    fi
    nohup python $mapfilegen_service &

    exit 0
fi


    


