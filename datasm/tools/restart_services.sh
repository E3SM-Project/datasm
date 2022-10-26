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

extraction_is_running=`ps aux | grep "python $extraction_service" | grep -v grep | wc -l`
mapfilegen_is_running=`ps aux | grep "python $mapfilegen_service" | grep -v grep | wc -l`

ts=`date -u +%Y%m%d_%H%M%S_%6N`

if [ $1 == "extraction" ]; then

    if [ $extraction_is_running -ne 0 ]; then
        echo "Archive Extraction Service is already running"
        exit 0
    fi    
    mkdir -p $userpath/Pub_Work/0_Extraction
    cd $userpath/Pub_Work/0_Extraction
    # clean up logs, then
    if [ -f nohup.out ]; then
        mv nohup.out runlogs/nohup.out-$ts
    fi
    nohup python $extraction_service &
    echo "Archive Extraction Service is started"

    exit 0
fi

if [ $1 == "mapfilegen" ]; then

    if [ $mapfilegen_is_running -ne 0 ]; then
        echo "Mapfile Generation Service is already running"
        exit 0
    fi    
    cd $userpath/Pub_Work/2_Mapwork
    # clean up logs, then
    if [ -f nohup.out ]; then
        mv nohup.out Runlogs/nohup.out-$ts
    fi
    nohup python $mapfilegen_service &
    echo "Mapfile Generation Service is started"

    exit 0
fi


    


