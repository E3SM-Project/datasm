#!/bin/bash

self=`whoami`
slurm_history=`$DSM_GETPATH USER_ROOT`/$self/Operations/6_DatasetPublication/slurm_history
publisher_logs=`$DSM_GETPATH USER_ROOT`/$self/Operations/6_DatasetPublication/Pub_Logs

mv slurm_scripts-* $slurm_history
mv Publication_Log-* $publisher_logs
rm DataSM.log-*
