#!/bin/bash

# The ONLY input parameter
in_cmip_dsid=$1

dry_run=1

# external dependencie
get_cmip6_parms=/p/user_pub/e3sm/staging/tools/cmip6_parameters.sh
force_status=/p/user_pub/e3sm/staging/tools/ensure_status_file_for_dsid.sh
list_e3sm_dsids=/p/user_pub/e3sm/staging/tools/list_e3sm_dsids.py
meta_version=/p/user_pub/e3sm/staging/tools/metadata_version.py

resource_path=/p/user_pub/e3sm/staging/resource
var_trans_table=$resource_path/table_cmip_var_to_e3sm
tables_path=$resource_path/cmor/cmip6-cmor-tables/Tables/
vrt_map_path=$resource_path/maps/vrt_remap_plev19.nc

E3SM_3D_Vars="O3,OMEGA,Q,RELHUM,T,U,V,Z3"

# reference full path to working directory
workdir=`pwd`
workdir=`realpath $workdir`
echo "workdir            = $workdir"

ts=`date -u +%Y%m%d_%H%M%S_%6N`
mkdir /tmp/$ts
$get_cmip6_parms $in_cmip_dsid > /tmp/$ts/cmip6parms

latest_native_data=`grep input_data /tmp/$ts/cmip6parms | cut -f2 -d:`
hrz_atm_map_path=`grep hrz_atm_map_path /tmp/$ts/cmip6parms | cut -f2 -d:`
mapfile=`grep mapfile /tmp/$ts/cmip6parms | cut -f2 -d:`
region_file=`grep region_file /tmp/$ts/cmip6parms | cut -f2 -d:`
file_pattern=`grep file_pattern /tmp/$ts/cmip6parms | cut -f2 -d:`
case_finder=`grep case_finder /tmp/$ts/cmip6parms | cut -f2 -d:`
namefile=`grep namefile_data /tmp/$ts/cmip6parms | cut -f2 -d:`
restart_path=`grep restart_data /tmp/$ts/cmip6parms | cut -f2 -d:`     # grab mpaso restart, even for sea-ice

parent_realm=`grep parent_native_dsid /tmp/$ts/cmip6parms | cut -f2 -d: | cut -f5 -d.`

rm -rf /tmp/$ts

expath=$workdir/the_cmorized

cmip_model_version=`echo $in_cmip_dsid | cut -f4 -d.`
cmip_experiment=`echo $in_cmip_dsid | cut -f5 -d.`
cmip_variant=`echo $in_cmip_dsid | cut -f6 -d.`
cmip_realm=`echo $in_cmip_dsid | cut -f7 -d.`
cmip_var=`echo $in_cmip_dsid | cut -f8 -d.`

parameter_path=$workdir/cwl_params
parameter_file=$parameter_path/${in_cmip_dsid}-${cmip_var}.yaml

major_model=`echo $cmip_model_version | cut -f2 -d-`
metadata_name="${cmip_experiment}_${cmip_variant}.json"
metadata=/p/user_pub/e3sm/staging/resource/CMIP6-Metadata/$cmip_model_version/$metadata_name
# UPDATE dataset version in metadata
cp $metadata $parameter_path
metadata=$parameter_path/$metadata_name
python $meta_version -i $metadata -m set

# get variable info
cmip_vars=$cmip_var
natv_vars=`cat $var_trans_table | grep ${cmip_var}: | cut -f2 -d:`


# INITIATE the input job.yaml config file with standard entries

rm -f $parameter_file
echo "  account: e3sm" >> $parameter_file
echo "  partition: debug" >> $parameter_file
echo "  num_workers: 12" >> $parameter_file
echo "  tables_path: $tables_path" >> $parameter_file

# Everyone loves Data
echo "  data_path: $latest_native_data" >> $parameter_file

# from cmip6_parameters.sh
echo "  hrz_atm_map_path: $hrz_atm_map_path" >> $parameter_file
echo "  mapfile: $mapfile" >> $parameter_file
echo "  region_file: $region_file" >> $parameter_file
echo "  file_pattern: $file_pattern" >> $parameter_file
echo "  case_finder: $case_finder" >> $parameter_file

# only needed for MPAS (ocean and sea-ice), but no harm in adding them generally
echo "  namelist_path: $namefile" >> $parameter_file
echo "  restart_path: $restart_path" >> $parameter_file

# Modulate the input job.yaml config file according to realm and model

if [ $parent_realm == "atmos" ]; then
    if [ echo ",$E3SM_3D_Vars," | grep -q ",$natv_vars," ]; then
        cwl_tail_path="atm-mon-plev/atm-plev.cwl"
    else
        cwl_tail_path="atm-mon-model-lev/atm-std.cwl"
    fi
    std_cmor_list=$cmip_vars
    std_var_list=$natv_vars

    # write the remaining job.yaml file
    echo "  vrt_map_path: $vrt_map_path" >> $parameter_file
    echo "  frequency: 50" >> $parameter_file
    echo "  e2c_timeout: 36000" >> $parameter_file
    echo "  slurm_timeout: '2-00:00'" >> $parameter_file
    echo "  metadata_path: $metadata" >> $parameter_file
    echo "  std_cmor_list:" >> $parameter_file
    echo "  - $std_cmor_list" >> $parameter_file
    echo "  std_var_list:" >> $parameter_file
    echo "  - $std_var_list" >> $parameter_file

elif [ $parent_realm == "land" ]; then
    cwl_tail_path=lnd-elm/lnd.cwl
    cmor_var_list=$cmip_vars
    lnd_var_list=$natv_vars

    # write the remaining job.yaml file
    echo "  frequency: 50" >> $parameter_file
    echo "  e2c_timeout: 36000" >> $parameter_file
    echo "  slurm_timeout: '1-00:00'" >> $parameter_file
    echo "  metadata_path: $metadata" >> $parameter_file
    echo "  cmor_var_list:" >> $parameter_file
    echo "  - $cmor_var_list" >> $parameter_file
    echo "  lnd_var_list:" >> $parameter_file
    echo "  - $lnd_var_list" >> $parameter_file

elif [ $parent_realm == "ocean" ]; then
    cwl_tail_path=mpaso/mpaso.cwl

    # write the remaining job.yaml file
    if [ $cmip_vars == "zhalfo" ]; then
        echo "  frequency: 5" >> $parameter_file
    else
        echo "  frequency: 10" >> $parameter_file
    fi
    echo "  e2c_timeout: 3456000" >> $parameter_file
    echo "  slurm_timeout: '4-00:00'" >> $parameter_file
    echo "  metadata: { 'class': 'File', 'path': $metadata }" >> $parameter_file 
    echo "  workflow_output: $expath" >> $parameter_file
    echo "  cmor_var_list: [ $cmip_vars ]" >> $parameter_file

elif [ $parent_realm == "sea-ice" ]; then
    cwl_tail_path=mpassi/mpassi.cwl

    # write the remaining job.yaml file
    echo "  frequency: 10" >> $parameter_file
    echo "  timeout: 36000" >> $parameter_file
    echo "  metadata: { 'class': 'File', 'path': $metadata }" >> $parameter_file 
    echo "  workflow_output: $expath" >> $parameter_file
    echo "  cmor_var_list: [ $cmip_vars ]" >> $parameter_file

else # river?
    echo "RIVER ???"
    exit 0
fi

cwl_workflow_path=/p/user_pub/e3sm/staging/resource/cwl_workflows/$cwl_tail_path

mkdir -p cwl_params

tmpdir_prefix=$workdir/ztmp/
echo "tmpdir_prefix: $tmpdir_prefix"

# cmd="cwltool --outdir $expath --tmpdir-prefix=$tmpdir_prefix --preserve-environment UDUNITS2_XML_PATH $cwl_workflow_path $parameter_file"
cmd="cwltool --outdir $expath --tmpdir-prefix=$tmpdir_prefix --leave-tmpdir --preserve-environment UDUNITS2_XML_PATH $cwl_workflow_path $parameter_file"

echo "CMD = $cmd"

if [ $dry_run -eq 1 ]; then
    exit 0
fi

ts=`date -u +%Y%m%d_%H%M%S_%6N`

logfile="log-$in_cmip_dsid-$ts"

$cmd > $logfile 2>&1



