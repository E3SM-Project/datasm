#!/bin/bash

staging_data=`$DSM_GETPATH STAGING_DATA`
publication=`$DSM_GETPATH PUBLICATION_DATA`
staging_resource=`$DSM_GETPATH STAGING_RESOURCE`
staging_status=`$DSM_GETPATH STAGING_STATUS`
archive_storage=`$DSM_GETPATH ARCHIVE_STORAGE`

cat << _EOF_
### WARNING - DO NOT EDIT DIRECTLY - changes can be overwritten.    ###
### USE (resource)/datasm_config.template.sh to modity and recreate ###

DEFAULT_WAREHOUSE_PATH: $staging_data/
DEFAULT_PUBLICATION_PATH: $publication/
DEFAULT_ARCHIVE_PATH: $archive_storage/
DEFAULT_STATUS_PATH: $staging_status/
DEFAULT_PLOT_PATH: /var/www/acme/acme-diags/baldwin32/cmip_verification

grids:
  v1_ne30_to_180x360: $staging_resource/maps/map_ne30np4_to_cmip6_180x360_aave.20181001.nc
  v2_ne30_to_180x360: $staging_resource/maps/map_ne30pg2_to_cmip6_180x360_aave.20200201.nc
  v1_oEC60to30_to_180x360: $staging_resource/maps/map_oEC60to30v3_to_cmip6_180x360_aave.20181001.nc
  v2_oEC60to30_to_180x360: $staging_resource/maps/map_EC30to60E2r2_to_cmip6_180x360_aave.20220301.nc
  v1_ne120np4_to_cmip6_180x360: $staging_resource/maps/map_ne120np4_to_cmip6_180x360_aave.20181001.nc
  v1_ne120np4_to_cmip6_720x1440: $staging_resource/maps/map_ne120np4_to_cmip6_720x1440_aave.20181001.nc
  v2_ne120pg2_to_cmip6_180x360: $staging_resource/maps/map_ne120pg2_to_cmip6_180x360_aave.20200201.nc
  v2_ne120pg2_to_cmip6_720x1440: $staging_resource/maps/map_ne120pg2_to_cmip6_720x1440_aave.20200201.nc
  v2_ne120_to_cmip6_180x360: $staging_resource/maps/map_ne120_to_cmip6_180x360_aave.20200901.nc
  v2_ne120_to_cmip6_720x1440: $staging_resource/maps/map_ne120_to_cmip6_720x1440_aave.20200901.nc

e3sm_resource_path: $staging_resource/
e3sm_gridfile_path: $staging_resource/grids/
e3sm_map_file_path: $staging_resource/maps/
e3sm_namefile_path: $staging_resource/namefiles/
e3sm_restarts_path: $staging_resource/restarts/
cmip_metadata_path: $staging_resource/CMIP6-Metadata/
cwl_workflows_path: $staging_resource/cwl_workflows/
cmip_tables_path: $staging_resource/cmor/cmip6-cmor-tables/Tables/
vrt_map_path: $staging_resource/grids/vrt_remap_plev19.nc

cmip_atm_mon:
  frequency: 50
  num_workers: 12
  account: e3sm
  partition: debug
  e2c_timeout: 36000
  slurm_timeout: "2-00:00"

cmip_atm_day:
  frequency: 10
  num_workers: 12
  account: e3sm
  partition: debug
  e2c_timeout: 36000
  slurm_timeout: "2-00:00"
  sample_freq: day
  time_steps_per_day: "1"

cmip_atm_3hr:
  frequency: 10
  num_workers: 12
  account: e3sm
  partition: debug
  e2c_timeout: 36000
  slurm_timeout: "2-00:00"
  sample_freq: 3hr
  time_steps_per_day: "8"

cmip_lnd_mon:
  frequency: 50
  num_workers: 12
  account: e3sm
  partition: debug
  e2c_timeout: 36000
  slurm_timeout: "1-00:00"

cmip_ocn_mon:
  frequency: 10
  num_workers: 12
  account: e3sm
  partition: debug
  e2c_timeout: 345600
  slurm_timeout: "2-00:00"
  v1_mpas_region_path: $staging_resource/grids/oEC60to30v3_Atlantic_region_and_southern_transect.nc
  v2_mpas_region_path: $staging_resource/grids/EC30to60E2r2_mocBasinsAndTransects20210623.nc
_EOF_

