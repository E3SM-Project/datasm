#!/bin/bash

term=$1

thisuser=`whoami`
slurm_history=`$DSM_GETPATH USER_ROOT`/$thisuser/Operations/5_DatasetGeneration/slurm_history

for adir in `ls $slurm_history`; do
    for outlog in `ls $slurm_history/$adir | grep .out`; do
        # echo $adir/$outlog
        
        hits=`grep $term $slurm_history/$adir/$outlog | wc -l`
        if [ $hits -gt 0 ]; then
            echo slurm_history/$adir/$outlog
        fi
    done
done

