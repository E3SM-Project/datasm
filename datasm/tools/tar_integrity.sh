#!/bin/bash

indir=$1

for atar in `ls $indir/*.tar`; do
    reply=`file $atar`
    found=`echo $reply | grep "POSIX tar archive" | wc -l`
    if [[ $found -eq 0 ]]; then
        echo "ERROR: $atar: $reply"
    fi
done
