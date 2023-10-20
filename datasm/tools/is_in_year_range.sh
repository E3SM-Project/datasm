#!/bin/bash

IFS=$'\n'

if [ $# -lt 3 ]; then
    echo "usage: $0 <datafile> <start_year> <end_year>"
    echo "(assumes date-section begins nnnn-nn)"
    exit 0
fi

afile=$1
start_yr=$2
final_yr=$3

# if [[ $aline =~ (*[0-9]{4}-[0-9]{2}*) ]]; then
if [[ $afile =~ -*([0-9]{4}-[0-9]{2}) ]]; then

    date_part=${BASH_REMATCH[1]}

    year=${date_part:0:4}

    if [[ $year -ge $start_yr && $year -le $final_yr ]]; then
        echo 1
    else
        echo 0
    fi
else
    echo 0
fi

exit

