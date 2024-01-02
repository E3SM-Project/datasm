#!/bin/bash

excess_spec=$1

for aline in `cat $excess_spec`; do
    acode=`echo $aline | cut -f1 -d:`
    apath=`echo $aline | cut -f2 -d:`
    if [ $acode == "EXCESS" ]; then
        rm -f $apath
    fi
done
