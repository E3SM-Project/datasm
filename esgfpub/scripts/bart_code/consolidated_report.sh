#!/bin/bash

work_dir=/p/user_pub/e3sm/bartoletti1/Pub_Status/

# awps_gen=$work_dir/awps_dataset_status-202104.py
conrep_gen=$work_dir/consolidated_dataset_report.py

sprokdir=$work_dir/sproket

esgf_rep=`ls $sprokdir | grep ESGF_publication_report- | tail -1`

ts=`date +%Y%m%d_%H%M%S`

final_rep=Consolidated_E3SM_Dataset_Status_Report-${ts}.csv

python $conrep_gen -s $sprokdir/$esgf_rep > $final_rep

# stattypes=`cat $finalrep | cut -f1 -d: | sort | uniq`
# 
# for atype in $stattypes; do
    # tcount=`cat $finalrep | grep $atype | wc -l`
    # stype=`echo $atype | cut -f2 -d=`
    # echo "$stype: $tcount datasets" >> AWPS_Status_Summary-$ts
# done

echo "Consolidated dataset report completed: $final_rep"

exit 0
