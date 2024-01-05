#!/bin/bash

self=`whoami`
ops_root=`$DSM_GETPATH USER_ROOT`/$self/Operations/4_DatasetValidation

ops_root=/p/user_pub/e3sm/bartoletti1/Operations/4_DatasetValidation

mv slurm_scripts-* $ops_root/slurm_history/
mv Validation_Log-* $ops_root/validation_logs/
