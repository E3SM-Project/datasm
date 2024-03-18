import os
from pathlib import Path

import yaml
from datasm.workflows.jobs import WorkflowJob
from datasm.util import log_message, get_first_nc_file, get_e2c_info, parent_native_dsid, latest_data_vdir, prepare_cmip_job_metadata, derivative_conf

NAME = 'GenerateOcnFixedCMIP'


class GenerateOcnFixedCMIP(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = {'ocean-native-mon': None}
        self._cmd = ''

    def resolve_cmd(self):

        target_dsid = self.dataset.dataset_id
        parent_dsid = parent_native_dsid(target_dsid)

        cwl_config = self.config['cmip_ocn_mon']
        metadata_path = self.config['cmip_metadata_path']
        resource_path = self.config['e3sm_resource_path']
        tables_path = self.config['cmip_tables_path']

        deriv_conf = derivative_conf(target_dsid, resource_path)

        data_path = latest_data_vdir(parent_dsid)
        datafile = get_first_nc_file(data_path)
        filepath = os.path.join(data_path, datafile)
        data_path_dict = { 'class': 'File', 'path': filepath }
        outpath = self.config['DEFAULT_WAREHOUSE_PATH']

        metadata_file = prepare_cmip_job_metadata(target_dsid, metadata_path, self._slurm_out)

        _, _, institution, cmip_model_version, experiment, variant, table, cmip_var, _ = self.dataset.dataset_id.split('.')

        # secure the target and native variables via e3sm_to_cmip --info

        cmip_out = os.path.join(self._slurm_out, "CMIP6")
        freq = parent_dsid.split('.')[7]
        realm = "Ofx"
        info_realm = "mpaso"
        if table == "SImon":
            info_realm = "mpassi"

        var_info = get_e2c_info(cmip_var, freq, realm, data_path, cmip_out, metadata_file, tables_path)

        # build up parameters list for job config .yaml write

        parameters = dict()

        parameters['tables_path'] = tables_path
        parameters['data_path'] = data_path
        parameters['ocn_data_path'] = data_path_dict
        parameters['metadata_file'] = metadata_file

        parameters['std_var_list']  = var_info['natv_vars']
        parameters['cmor_var_list'] = var_info['cmip_vars']

        parameters.update(cwl_config)   # obtain frequency, num_workers, account, partition, e2c_timeout, slurm_timeout
        parameters.update(deriv_conf)   # obtain hrz_atm_map_path, mapfile, region_file, file_pattern, case_finder

        log_message("info", f"Obtained e3sm_vars: {', '.join(e3sm_vars)}")
        log_message("info", f"Obtained cmip_vars: {', '.join(cmip_vars)}")


        # write out the parameter file and setup the temp directory
        parameters_name = f"{experiment}-{cmip_model_version}-{variant}-{realm}-cmip-{freq}-{cmip_var}.yaml"
        parameters_path = os.path.join( self._slurm_out, parameters_name)
        with open(parameters_path, 'w') as outstream:
            yaml.dump(parameters, outstream)

        cwl_workflow_main = "Ofx/Ofx.cwl"
        cwl_workflow_path = os.path.join(self.config['cwl_workflows_path'], cwl_workflow_main)
        
        if not self.serial:
            parallel = "--parallel"
        else:
            parallel = ''

        self._cmd = f"cwltool --outdir {outpath} --tmpdir-prefix={self.tmpdir} {parallel} --preserve-environment UDUNITS2_XML_PATH {cwl_workflow_path} {parameters_path}"


#
# e2c command-line for Ofx generation:

# e3sm_to_cmip -s --realm Ofx --var-list areacello --map [STAGING_RESOURCE]/maps/map_EC30to60E2r2_to_cmip6_180x360_aave.20220301.nc --input-path [USER_ROOT]/zhang40/e2c_tony/e2c_test_data/v2.mpassi_input/ --output-path [USER_ROOT]/zhang40/tests/ncremap_sgs  --user-metadata [USER_ROOT]/zhang40/e2c_tony/e2c_test_data/holodeck/input/historical_r1i1p1f1.json --tables-path [STAGING_RESOURCE]/cmor/cmip6-cmor-tables/Tables

#  

