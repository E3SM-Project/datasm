#!/bin/bash

dsid=$1

log_history=`$DSM_GETPATH USER_ROOT`/bartoletti1/Operations/5_DatasetGeneration/log_history
slurm_history=`$DSM_GETPATH USER_ROOT`/bartoletti1/Operations/5_DatasetGeneration/slurm_history

ts=`date -u +%Y%m%d_%H%M%S_%6N`

echo "PostProcess_Logs:"
ls $log_history | grep $dsid > tmp-$ts

for aline in `cat tmp-$ts`; do
    echo "$log_history/$aline"
done

rm -f tmp-$ts

echo ""
echo "SlurmDirectories:"
ls $slurm_history | grep $dsid > tmp-$ts

for aline in `cat tmp-$ts`; do
    echo "$slurm_history/$aline"
    for item in `ls $slurm_history/$aline`; do
        echo "    $item"
    done
done

rm -f tmp-$ts
