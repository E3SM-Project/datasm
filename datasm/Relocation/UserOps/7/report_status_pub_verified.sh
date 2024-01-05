#!/bin/bash

statusdir=`$DSM_GETPATH STAGING_STATUS`

ts=`date -u +%Y%m%d_%H%M%S_%6N`
result="result_Pub_Verified-$ts"

for afile in `ls $statusdir`; do
    echo -n $afile >> $result
    
    stat=`grep "PUBLICATION:Verified" $statusdir/$afile | tail -1`

    if [ -z $stat ]; then
        echo "" >> $result
        continue
    fi
    echo $stat >> $result
done



