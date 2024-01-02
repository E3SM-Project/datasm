#!/bin/bash

dsids=$1

for dsid in `cat $dsids`; do
    lastdir=`ls slurm_history | grep $dsid | tail -1`
    # echo "LASTDIR = $lastdir"
    if [ -d slurm_history/$lastdir ]; then
        ls -l slurm_history/$lastdir
    else
        echo "$dsid: No Slurm Directories found"
    fi
done

