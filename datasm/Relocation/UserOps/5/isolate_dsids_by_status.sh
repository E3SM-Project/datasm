#!/bin/bash

workdir=`pwd`
thisdir=`realpath $workdir`

for opsdir in Ops1 Ops2 Ops3 Ops4 Ops5 Ops6 Ops7 Ops8 Ops9; do
    cd $thisdir/$opsdir;
    logs=`ls | grep PostProcess_Log`
    rm -f recent_Pass recent_Fail recent_Work
    for alog in $logs; do
        dsid=`echo $alog | cut -f3- -d-`
        res_Pass=`cat $alog | grep "is in state" | grep Pass`
        res_Fail=`cat $alog | grep "is in state" | grep Fail`
        if [[ ! -z $res_Pass ]]; then
            # echo $dsid: res_Pass = $res_Pass
            echo $dsid >> recent_Pass
        elif [[ ! -z $res_Fail ]]; then
            # echo $dsid: res_Fail = $res_Fail
            echo $dsid >> recent_Fail
        else
            # echo $dsid: Unfinished
            echo $dsid >> recent_Work
        fi
    done
done
