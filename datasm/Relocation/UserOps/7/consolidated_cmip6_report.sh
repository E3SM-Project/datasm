#!/bin/bash

thisuser=`whoami`
work_dir=`$DSM_GETPATH USER_ROOT`/$thisuser/Operations/7_StatusReporting
conrep_gen=`$DSM_GETPATH STAGING_TOOLS`/consolidated_cmip_dataset_report.py

ts=`date -u +%Y%m%d_%H%M%S_%6N`

final_rep=$workdir/Reports/Consolidated_CMIP6_Dataset_Status_Report-${ts}.csv

python $conrep_gen > $final_rep 2> log_stderr
# python $conrep_gen --unrestricted > $final_rep
cat $final_rep | grep -v "test,test" | grep -v "DEBUG:" > xxy
mv xxy $final_rep

echo "Consolidated CMIP6 dataset report completed: $final_rep"

exit 0
