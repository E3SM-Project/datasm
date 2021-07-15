#!/bin/bash

work_dir=/p/user_pub/e3sm/bartoletti1/Pub_Status/

# awps_gen=$work_dir/awps_dataset_status-202104.py
conrep_gen=$work_dir/consolidated_dataset_report.py

ts=`date +%Y%m%d_%H%M%S`

final_rep=Consolidated_E3SM_Dataset_Status_Report-${ts}.csv

python $conrep_gen > $final_rep

echo "Consolidated dataset report completed: $final_rep"

exit 0
