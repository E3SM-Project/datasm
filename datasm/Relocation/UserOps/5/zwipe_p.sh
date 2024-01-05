#!/bin/bash

ops_root=`$DSM_GETPATH USER_ROOT`/bartoletti1/Operations/5_DatasetGeneration

rm -rf slurm_scripts-*
rm -rf slurm_scripts
rm -rf logs
rm -f PostProcess_Log-*
rm -f DataSM.log-*
rm -f e3sm_to_cmip.util*
rm -f nohup.out
