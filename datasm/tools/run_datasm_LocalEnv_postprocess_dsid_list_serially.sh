#!/bin/bash

usage="run_datasm_postprocess_dsid_list_serially.sh file_of_dataset_ids [alt_path_to_dataset_spec]"

# NOTE: this version relies upon [STAGING_TOOLS]/latest_data_location_by_dsid.sh to select the src_root

# obtain global relocatable paths

dsm_tools=`$DSM_GETPATH STAGING_TOOLS`
resource_path=`$DSM_GETPATH STAGING_RESOURCE`
wh_root=`$DSM_GETPATH STAGING_DATA`
pb_root=`$DSM_GETPATH PUBLICATION_DATA`
user_root=`$DSM_GETPATH USER_ROOT`

inlist=$1

ds_spec=$resource_path/dataset_spec.yaml

if [ $# -eq 2 ]; then
    ds_spec=$2
fi

datalocator=$dsm_tools/datasm_pp_sourceroot.sh

src_root=""
dst_root=$wh_root

# to limit total processes
workers=30

# ulimit -u 8192

OpsDir=`basename \`pwd\``

user=`whoami`
user_path=$user_root/$user


for dsid in `cat $inlist`; do
    if [ ${dsid:0:1} == "#" ]; then
        continue
    fi

    ts1=`date -u +%Y%m%d_%H%M%S_%6N`

    if [ -f HALT_ON_DSID ]; then
        h_dsid=`head -1 HALT_ON_DSID`
        if [ $dsid == $h_dsid ]; then
            cp HALT_ON_DSID HALTED_ON_DSID-$ts
            exit 0
        fi
    fi

    mkdir -p $user_path/Operations/5_DatasetGeneration
    postproc_log=$user_path/Operations/5_DatasetGeneration/$OpsDir/PostProcess_Log-$ts1-$dsid

    datasrc=`$datalocator $dsid`
    if [ $datasrc == "NONE" ]; then
        echo "$ts1: No Data located for $dsid: ... BUMMER !  Need Native data, not current dsid - Argggh!" >> $postproc_log
        continue
    fi
    if [ $datasrc == "publication" ]; then
        src_root=$pb_root
    else
        src_root=$wh_root
    fi
    
    # rm -rf $TMPDIR
    mkdir -p $TMPDIR

    echo "$ts1: preparing to post_process: $dsid" >> $postproc_log
    # the_cmd="datasm postprocess -w $src_root --native-srcroot $src_root -p $dst_root --job-workers $workers -d $dsid"
    the_cmd="datasm postprocess -w $src_root -p $dst_root --job-workers $workers -d $dsid --dataset-spec $ds_spec"
    echo "$ts1: ISSUING:  $the_cmd"
    echo "$ts1: ISSUING:  $the_cmd" >> $postproc_log

    $the_cmd >> $postproc_log 2>&1
    retcode=$?

    ts2=`date -u +%Y%m%d_%H%M%S_%6N`
    echo "$ts2: DataSM Postprocess: retcode = $retcode" >> $postproc_log
    if [ $retcode -ne 0 ]; then
        echo "$ts2: PostProcess: $dsid: POSTPROCESS:Fail" >> $postproc_log
    fi
    # retain the slurm directory
    mv slurm_scripts slurm_scripts-$ts1-$dsid
    echo "$ts2: Completed PostProcess: $dsid"
    echo "$ts2: Completed PostProcess: $dsid" >> $postproc_log
done




