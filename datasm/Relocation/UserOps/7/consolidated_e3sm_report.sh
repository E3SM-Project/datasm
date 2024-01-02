#!/bin/bash

thisuser=`whoami`
work_dir=`$DSM_GETPATH USER_ROOT`/$thisuser/Operations/7_StatusReporting
conrep_gen=`$DSM_GETPATH STAGING_TOOLS`/consolidated_e3sm_dataset_report.py

ts=`date -u +%Y%m%d_%H%M%S_%6N`

final_rep=$workdir/Reports/Consolidated_E3SM_Dataset_Status_Report-${ts}.csv

python $conrep_gen > $final_rep
cat $final_rep | grep -v "test,test" | grep -v "DEBUG:" > xxx
mv xxx $final_rep

echo "Consolidated E3SM dataset report completed: $final_rep"

exit 0
