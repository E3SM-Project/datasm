#!/bin/bash

infile=`ls Consolidated_E3SM_Dataset_Status_Report-*.csv | sort | tail -1`

# 13: P
# 14: S
# 19: P_Version
# 20: P_Count
# 21: S_Version
# 22: S_Count
# 24: PublicationPath

ts=`date +%Y%m%d_%H%M%S`

output="Anomaly_Report-$ts"

for aline in `cat $infile`; do
    P=`echo $aline | cut -f13 -d,`
    S=`echo $aline | cut -f14 -d,`
    if [ $P == "P" ] && [ $S == "S" ]; then
        P_Vers=`echo $aline | cut -f19 -d,`
        S_Vers=`echo $aline | cut -f21 -d,`
        P_Count=`echo $aline | cut -f20 -d,`
        S_Count=`echo $aline | cut -f22 -d,`
        P_Path=`echo $aline | cut -f24 -d,`
        if [ X$P_Vers != X$S_Vers ]; then
            echo "P_Vers=$P_Vers,S_Vers=$S_Vers,path=$P_Path" >> $output
        fi
        if [ X$P_Count != X$S_Count ]; then
            echo "P_Count=$P_Count,S_Count=$S_Count,path=$P_Path" >> $output
        fi
    fi
done 
 
cat $output | sort > /tmp/$output
mv /tmp/$output $output

echo "Completed Anomaly Report from: $infile"
