#!/bin/bash

dsid=$1

for adir in `ls slurm_history`; do
    hits=`ls slurm_history/$adir | grep $dsid | wc -l`
    if [ $hits -gt 0 ]; then
        echo $adir
    fi
done

