#!/bin/bash

ops_root=`$DSM_GETPATH USER_ROOT`/bartoletti1/Operations/5_DatasetGeneration

mv slurm_scripts-* $ops_root/slurm_history/
mv PostProcess_Log-* $ops_root/log_history/
mv DataSM.log-* $ops_root/log_history/
rm -f e3sm_to_cmip.util*
rm -rf logs
