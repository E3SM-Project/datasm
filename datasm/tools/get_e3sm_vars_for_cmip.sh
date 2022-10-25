#!/bin/bash

invar=$1

tmp_out=/tmp/pqrst999
touch $tmp_out

reset_env=0
env=`conda info | grep "active environment" | cut -f2 -d: | tr -d ' '`

if [ $env != "$ENV_DSM_PROC" ]; then
    echo "Setting conda environment to $ENV_DSM_PROC"
    source ~/anaconda3/etc/profile.d/conda.sh
    conda activate $ENV_DSM_PROC
    reset_env=1
fi

e3sm_to_cmip --info -v $invar --info-out $tmp_out >/dev/null 2>&1

cat $tmp_out | grep "E3SM Variables" | cut -f2 -d: | sed -e 's/ //g'

rm -f $tmp_out

if [ $reset_env -eq 1 ]; then
    conda deactivate
fi


