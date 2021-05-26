#!/bin/bash

dirlist=$1

env=`conda info | grep "active environment" | cut -f2 -d: | tr -d ' '`
# opython=`python --version`
# echo "ENV_STUFF: env = $env: python version = $opython"

reset_env=0

if [ $env != "rect" ]; then
    echo "Setting conda environment to rect"
    source ~/anaconda3/etc/profile.d/conda.sh
    conda activate rect
    reset_env=1
fi

ts=`date +%Y%m%d_%H%M%S`

list_success="List_Success-$ts"
list_failure="List_Failure-$ts"
touch $list_success
touch $list_failure

for srcdir in `cat $dirlist`; do
    ts=`date +%Y%m%d_%H%M%S`
    ens_path=`dirname $srcdir`
    statfile=$ens_path/.status
    echo "STAT:$ts:WAREHOUSE:VALIDATION:Engaged" >> $statfile
    echo "STAT:$ts:VALIDATION:CHECKUNITS:Engaged" >> $statfile

    python checkUnits.py -p 8 --time-name time -q $srcdir >> rlog_stdout_checkunits 2>> rlog_stderr_checkunits
    retval=$?
    if [ $retval -eq 0 ]; then
        echo "STAT:$ts:VALIDATION:CHECKUNITS:Pass" >> $statfile
        echo "STAT:$ts:VALIDATION:CHECKTIME:Engaged" >> $statfile
        python checkTime.py -j 8 -q $srcdir >> rlog_stdout_checktime 2>> rlog_stderr_checktime
        retval=$?
        if [ $retval -eq 0 ]; then
            echo "$srcdir" >> $list_success
            echo "STAT:$ts:VALIDATION:CHECKTIME:Pass" >> $statfile
            echo "STAT:$ts:WAREHOUSE:VALIDATION:Pass" >> $statfile
        else
            echo "$srcdir" >> $list_failure
            echo "STAT:$ts:VALIDATION:CHECKTIME:Fail" >> $statfile
        fi
    else
        echo "$srcdir" >> $list_failure
        echo "STAT:$ts:VALIDATION:CHECKUNITS:Fail" >> $statfile
    fi
        
done

exit 0


if [ $reset_env -eq 1 ]; then
    conda deactivate
fi

