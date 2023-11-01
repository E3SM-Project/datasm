#!/bin/bash

invar=$1

ts=`date -u +%Y%m%d_%H%M%S_%6N`
tmp_out=/tmp/v_info-$ts
touch $tmp_out

e3sm_to_cmip --info -v $invar --info-out $tmp_out >/dev/null 2>&1

cat $tmp_out | grep "E3SM Variables" | cut -f2 -d: | sed -e 's/ //g'

rm -f $tmp_out



