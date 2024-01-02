#!/bin/bash

usage="run_with_spec.sh <list_of_dataset_ids>  (Ensure local DS_Spec file indicates correct dataset_spec)"

if [ $# -eq 0 ]; then
    echo $usage
    exit 0
fi

here=`pwd`
opdir=`basename $here`
if [ ${opdir:0:3} != "Ops" ]; then
    echo "Please execute script from a dedicated \"Ops\" subdirectory to isolate runtime slurm_scripts."
    exit 1
fi

dsidlist=$1

ds_spec=`grep -v \# DS_Spec`

pp_script=/p/user_pub/e3sm/staging/tools/run_datasm_LocalEnv_postprocess_dsid_list_serially.sh

$pp_script $dsidlist $ds_spec


