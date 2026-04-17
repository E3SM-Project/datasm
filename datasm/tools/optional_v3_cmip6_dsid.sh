#!/bin/bash

cmip_dsid=$1

# Prepare for v3 data dsid substitution
eff_dsid=$cmip_dsid
testkey=`echo $cmip_dsid | cut -f1,4 -d.`
if [[ $testkey == "CMIP6.E3SM-3-0" ]]; then
    tailpart=`echo $cmip_dsid | cut -f2- -d.`
    eff_dsid="CMIP6-E3SM-Ext.$tailpart"
fi

echo $eff_dsid
