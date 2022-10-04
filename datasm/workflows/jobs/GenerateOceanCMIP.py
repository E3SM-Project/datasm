import os
from pathlib import Path
from subprocess import PIPE, Popen
from tempfile import NamedTemporaryFile

import yaml

from datasm.util import log_message, get_UTC_YMD, set_version_in_user_metadata
from datasm.workflows.jobs import WorkflowJob

NAME = 'GenerateOceanCMIP'

oa_vars = ['pbo', 'pso']

class GenerateOceanCMIP(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        log_message("info", f"INIT: set self.name = {NAME} self.dataset.dataset_id = {self.dataset.dataset_id}")
        cmip_var = self.dataset.dataset_id.split('.')[-2]
        if cmip_var in oa_vars:
            self._requires = { 'ocean-native-mon': None, 'atmos-native-mon': None }
        else:
            self._requires = { 'ocean-native-mon': None }
        self._cmd = ''

    def resolve_cmd(self):

        log_message("info", f"resolve_cmd: Start: dsid={self.dataset.dataset_id}")

        cwl_config = self.config['cmip_ocn_mon']

        _, _, _, model_version, experiment, variant, table, cmip_var, _ = self.dataset.dataset_id.split('.')

        is_oa_var = False
        if cmip_var in oa_vars:
            is_oa_var = True

        # Begin parameters collection:  use 'mpas_data_path' and 'atm_data_path' for mpaso-atm.
        # including atmos-native_mon as (pbo requires PSL, etc)
        if is_oa_var:
            raw_ocean_dataset = self.requires['ocean-native-mon']
            raw_atmos_dataset = self.requires['atmos-native-mon']
            parameters = { 'mpas_data_path': raw_ocean_dataset.latest_warehouse_dir, 'atm_data_path': raw_atmos_dataset.latest_warehouse_dir }
        else:
            raw_ocean_dataset = self.requires['ocean-native-mon']
            parameters = { 'data_path': raw_ocean_dataset.latest_warehouse_dir, }

        parameters.update(cwl_config)   # obtain frequency, num_workers, account, partition, timeout, slurm_timeout, mpas_region_path

        # log_message("info", f"resolve_cmd: Obtained model_version {model_version}, experiment {experiment}, variant {variant}, table {table}, cmip_var {cmip_var}")

        # if we want to run all the variables
        # we can pull them from the dataset spec
        if cmip_var == 'all':
            is_all = True
            cmip_var = [x for x in self._spec['tables'][table] if x != 'all']
        else:
            is_all = False
            cmip_var = [cmip_var]

        var_string = ', '.join(cmip_var)
        log_message("info", f"DBG: resolve_cmd: var_string = {var_string}")

        '''
        # Call E2C to obtain variable info
        info_file = NamedTemporaryFile(delete=False)
        cmd = f"e3sm_to_cmip --info --realm mpaso --map dummy -v {var_string} -t {self.config['cmip_tables_path']} --info-out {info_file.name}"
        log_message("info", f"E2C --info call: cmd = {cmd}")
        proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        _, err = proc.communicate()
        if err:
            log_message("error", f"resolve_cmd failed to obtain e3sm_to_cmip info {info_file.name}")
            log_message("error", f"  cmd = {cmd}")
            log_message("error", f"  err = {err}")
            return 1    # was return None

        with open(info_file.name, 'r') as instream:
            variable_info = yaml.load(instream, Loader=yaml.SafeLoader)
        if variable_info is None:
            log_message("error", f"Unable to find correct input variable for requested CMIP conversion, is {cmd} correct?")
            raise ValueError(f"Unable to find correct input variable for requested CMIP conversion, is {cmd} correct?")
            return 1    # was return None

        # I just want to see what "variable_info" is returned
        log_message("info", "==================================================================================")
        for item in variable_info:
            log_message("info", f"DBG: E2C INFO item = {item}")
        log_message("info", "==================================================================================")

        parameters['std_var_list'] = std_var_list
        # Apply variable info to parameters collection
        for item in variable_info:
            if isinstance(item['E3SM Variables'], list):
                log_message("info", f"DBG: extending std_var_list[] with {item['E3SM Variables']}")
                parameters['std_var_list'].extend([v for v in item['E3SM Variables']])
            elif isinstance(item['E3SM Variables'], str):
                log_message("info", f"DBG: extending std_var_list[] with {item['E3SM Variables']}")
                parameters['std_var_list'].extend([v for v in item['E3SM Variables'].split(', ')])

            std_cmor_list.append(item['CMIP6 Name'])

        log_message("info", f"DBG: resolve_cmd: obtained std_cmor_list = {std_cmor_list}")
        '''

        # Apply variable info to parameters collection

        if is_oa_var:
            parameters['std_var_list'] = ['PSL']
            parameters['mpas_var_list'] = cmip_var
            cwl_workflow_main = "mpaso-atm/mpaso-atm.cwl"
        else:
            parameters['cmor_var_list'] = cmip_var
            cwl_workflow_main = "mpaso/mpaso.cwl"

        parameters['tables_path'] = self.config['cmip_tables_path']
        cwl_workflow = os.path.join(self.config['cwl_workflows_path'], cwl_workflow_main)
        metadata_path = os.path.join(self.config['cmip_metadata_path'],model_version,f"{experiment}_{variant}.json")

        # force dataset output version here
        ds_version = "v" + get_UTC_YMD()
        set_version_in_user_metadata(metadata_path, ds_version)
        log_message("info", f"Set dataset version in {metadata_path} to {ds_version}")
        
        parameters['metadata'] = {
            'class': 'File',
            'path': metadata_path
            }

        if is_oa_var:
            parameters['metadata_path'] = parameters['metadata']

        parameters['region_path'] = parameters['mpas_region_path']
        parameters['mapfile'] = { 'class': 'File', 'path': self.config['grids']['oEC60to30_to_180x360'] }
        if model_version == "E3SM-2-0":
            parameters['hrz_atm_map_path'] = self.config['grids']['v2_ne30_to_180x360']
        else:
            parameters['hrz_atm_map_path'] = self.config['grids']['v1_ne30_to_180x360']


        if is_oa_var:
            parameters['mpas_map_path'] = self.config['grids']['oEC60to30_to_180x360']

        raw_model_version = raw_ocean_dataset.model_version
        raw_experiment = raw_ocean_dataset.experiment

        parameters['namelist_path'] = os.path.join(self.config['e3sm_namefile_path'],raw_model_version,raw_experiment,'mpaso_in')
        parameters['restart_path'] = os.path.join(self.config['e3sm_restarts_path'],raw_model_version,raw_experiment,'mpaso.rst.1851-01-01_00000.nc')
        parameters['workflow_output'] = '/p/user_pub/e3sm/warehouse'

        # step two, write out the parameter file and setup the temp directory
        var_id = 'all' if is_all else cmip_var[0]
        parameter_path = os.path.join(
            self._slurm_out, f"{self.dataset.experiment}-{self.dataset.model_version}-{self.dataset.ensemble}-ocn-cmip-mon-{var_id}.yaml")
        with open(parameter_path, 'w') as outstream:
            yaml.dump(parameters, outstream)

        # step three, render out the CWL run command
        # OVERRIDE : needed to be "pub_dir" to find the data, but back to "warehouse" to write results to the warehouse
        outpath = '/p/user_pub/e3sm/warehouse'  # was "self.dataset.warehouse_base", but -w <pub_root> for input selection interferes.

        if not self.serial:
            parallel = "--parallel"
        else:
            parallel = ''
        self._cmd = f"cwltool --outdir {outpath} --tmpdir-prefix={self.tmpdir} {parallel} --preserve-environment UDUNITS2_XML_PATH {cwl_workflow} {parameter_path}"
