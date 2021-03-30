#!/bin/bash

do_csv=0

if [ $# -eq 1 ]; then
    if [ $1 == "csv" ]; then
        do_csv=1
    fi
fi

work_dir=/p/user_pub/e3sm/bartoletti1/Pub_Status/

awps_gen=$work_dir/awps_dataset_status.py

sprokdir=$work_dir/sproket

esgf_rep=`ls $sprokdir | grep ESGF_publication_report- | tail -1`

ts=`date +%Y%m%d_%H%M%S`

out_temp="tempfile-$ts"
finalrep=AWPS_Status_Report-$ts
if [ $do_csv -eq 1 ]; then
    finalrep=AWPS_Status_Report-${ts}.csv
fi

if [ $do_csv -eq 1 ]; then
    python $awps_gen -s $sprokdir/$esgf_rep --csv > $out_temp
else
    python $awps_gen -s $sprokdir/$esgf_rep > $out_temp
fi

sort $out_temp | uniq | grep -v ____ > $finalrep

rm $out_temp

stattypes=`cat $finalrep | cut -f1 -d: | sort | uniq`

for atype in $stattypes; do
    tcount=`cat $finalrep | grep $atype | wc -l`
    stype=`echo $atype | cut -f2 -d=`
    echo "$stype: $tcount datasets" >> AWPS_Status_Summary-$ts
done

echo "AWPS report completed: $finalrep"

exit 0
