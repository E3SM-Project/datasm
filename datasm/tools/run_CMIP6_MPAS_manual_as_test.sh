#!/bin/bash

# Process Control Variables
verbose=1
dryrun=0
realrun=$((1 - dryrun))

if [[ $# -eq 0 || $1 == "-h" || $1 == "--help" ]]; then
    echo "Usage: $0 <cmip6_dataset_id> [ <start_year> <final_year> ]"
    exit 0
fi

if [[ $dryrun == 1 ]]; then
    echo "NOTE: Running in DRYRUN Mode.  Edit dryrun manually to change."
fi

# define a Boolean year-range testing function
# Usage: is_file_in_year_range <file> <start_yr> <final_yr>

is_file_in_year_range () {

    if [[ $# -ne 3 ]]; then
        echo 0
    fi

    afile=$1
    start_yr=$2
    final_yr=$3

    # if [[ $aline =~ (*[0-9]{4}-[0-9]{2}*) ]]; then
    if [[ $afile =~ -*([0-9]{4}-[0-9]{2}) ]]; then

        date_part=${BASH_REMATCH[1]}

        year=${date_part:0:4}

        if [[ $year -ge $start_yr && $year -le $final_yr ]]; then
            echo 1
        else
            echo 0
        fi
    else
        echo 0
    fi
}


ts=`date -u +%Y%m%d_%H%M%S_%6N`

## BEGIN PROCESSING INPUT
# reference input CMIP6 dataset_id
in_cmip_dsid=$1
if [ $verbose ]; then echo "input_dataset_id   = $in_cmip_dsid"; fi

limit_years=0

if [ $# -eq 3 ]; then
    start_yr=$2
    final_yr=$3
    limit_years=1
fi

# reference full path to working directory
workdir=`pwd`
workdir=`realpath $workdir`

# external dependencies

cmip6_params=/p/user_pub/e3sm/staging/tools/cmip6_parameters.sh
meta_version=/p/user_pub/e3sm/staging/tools/metadata_version.py

parent_dsid=`/p/user_pub/e3sm/staging/tools/parent_native_dsid.sh $in_cmip_dsid`

params_temp=params-$ts
$cmip6_params $in_cmip_dsid > $params_temp

latest_data_path=`cat $params_temp | grep input_data | cut -f2 -d:`
namefile=`cat $params_temp | grep namefile_data | cut -f2 -d:`
restart_file=`cat $params_temp | grep restart_data | cut -f2 -d:`
map_file=`cat $params_temp | grep mapfile | cut -f2 -d:`
region_file=`cat $params_temp | grep region_file | cut -f2 -d:`

rm -f $params_temp

# generate variable_name from CMIP6 dataset_id components
cmip_var_name=`echo $in_cmip_dsid | cut -f8 -d.`

# generate cmip_realm from CMIP6 dataset_id components
cmip_realm=`echo $in_cmip_dsid | cut -f7 -d.`

region_file_base=`basename $region_file`
namefile_base=`basename $namefile`
region_file_base=`basename $region_file`

# generate metadata_path from CMIP6 dataset_id components
exper_source=`echo $in_cmip_dsid | cut -f4 -d.`
exper_variant=`echo $in_cmip_dsid | cut -f5,6 -d. | tr . _`
metadata_path=/p/user_pub/e3sm/staging/resource/CMIP6-Metadata/$exper_source/${exper_variant}.json
echo "metadata_path      = $metadata_path"
meta_base=`basename $metadata_path`

# This is a constant - read-only
tables_path=/p/user_pub/e3sm/staging/resource/cmor/cmip6-cmor-tables/Tables

if [ $verbose ]; then
    echo "workdir            = $workdir"
    echo "cmip_realm         = $cmip_realm"
    echo "variable_name      = $cmip_var_name"
    echo "native_e3sm_dsid   = $parent_dsid"
    echo "latest_data_path   = $latest_data_path"
    echo "namefile           = $namefile"
    echo "restart_file       = $restart_file"
    echo "mapfile            = $map_file"
    echo "region_file        = $region_file"
    echo "tables_path        = $tables_path"
fi

if [ $latest_data_path == "NONE" ]; then
    echo "ERROR: no native datapath (warehouse or publication) found for native dataset_id $parent_dsid"
    exit 1
fi

tss=`date -u +%Y%m%d_%H%M%S`

# Create and Populate the holodeck
holodeck="$workdir/holodeck-$tss"
holodeck_in="$holodeck/input"
holodeck_out="$holodeck/output"
holodeck_log="$holodeck/log"

mkdir -p $holodeck_in $holodeck_out $holodeck_log

# Make symlinks in $holodeck_in for datafiles of $latest_data_path, PLUS  $namefile_path, $restart_file, $region_file,

cd $holodeck_in
cp $metadata_path .
metadata_path=$holodeck_in/$meta_base
echo "NEW metadata_path = $metadata_path"
ln -s $namefile $namefile_base
ln -s $restart_file $restart_base
ln -s $region_file $region_file_base

echo "Creating datafile symlinks in holodeck - this may take a minute . . ."
for afile in `ls $latest_data_path`; do
    if [[ $limit_years -eq 1 ]]; then
        if [[ `is_file_in_year_range $afile $start_yr $final_yr` -eq 1 ]]; then 
            ln -s $latest_data_path/$afile $afile
        fi
    else    # just do it
        ln -s $latest_data_path/$afile $afile
    fi
done
cd $holodeck

# Forge the E2C Command Line
cmd="e3sm_to_cmip -s --realm $cmip_realm --var-list $cmip_var_name --map $map_file --input-path $holodeck_in --output-path $holodeck_out --logdir $holodeck_log --user-metadata $metadata_path --tables-path $tables_path"

if [ $dryrun -eq 1 ]; then
    echo ""
    echo "DryRun: CMD:  $cmd"
    exit 0
fi

# Begin Process Execution =========================================

init_ts=`date -u +%Y%m%d_%H%M%S_%6N`

# force metadata version to be today's date
python $meta_version -i $metadata_path -m set
version=`python $meta_version -i $metadata_path -m get`

echo DECLARING version = $version

echo "$init_ts: Issuing CMD = $cmd"
if [ $realrun -eq 1 ]; then
    init_ts=`date -u +%Y%m%d_%H%M%S_%6N`
    echo "STAT:$init_ts:E3SM_TO_CMIP:Engaged"
    $cmd
    retcode=$?
fi

last_ts=`date -u +%Y%m%d_%H%M%S_%6N`

final_src=$holodeck_out/$dsidpath/$version

fcount=`ls $final_src | wc -l`
echo $last_ts: Process completed. Return code = $retcode.  Generated $fcount output files for dataset $in_cmip_dsid

if [ $realrun -eq 1 ]; then
    if [ $fcount -eq 0 ]; then
        echo "STAT:$last_ts:E3SM_TO_CMIP:Fail"
        exit 0
    fi
fi

