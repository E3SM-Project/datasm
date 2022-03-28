import os
import yaml
from pathlib import Path
from tempfile import NamedTemporaryFile
from subprocess import Popen, PIPE
from warehouse.workflows.jobs import WorkflowJob
from warehouse.util import log_message

NAME = 'GenerateOceanCMIP'


class GenerateOceanCMIP(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        # including atmos-native_mon as (pbo requires PSL)
        self._requires = {
            'ocean-native-mon': None,
            'atmos-native-mon': None    
        }
        self._cmd = ''

    def resolve_cmd(self):

        log_message("info", "resolve_cmp: Begin")

        # including atmos-native_mon as (pbo requires PSL)
        raw_ocean_dataset = self.requires['ocean-native-mon']
        raw_atmos_dataset = self.requires['atmos-native-mon']
        cwl_config = self.config['cmip_ocn_mon']

        # Begin parameters collection
        parameters = {
            'mpas_data_path': raw_ocean_dataset.latest_warehouse_dir,
            'atm_data_path': raw_atmos_dataset.latest_warehouse_dir
        }
        parameters.update(cwl_config)

        _, _, _, model_version, experiment, variant, table, cmip_var, _ = self.dataset.dataset_id.split('.')

        log_message("info", f"resolve_cmp: Obtained model_version {model_version}, experiment {experiment}, variant {variant}, table {table}, cmip_var {cmip_var}")

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

        # Call E2C to obtain variable info
        mapfile="/p/user_pub/e3sm/staging/resource/map_oEC60to30v3_to_cmip6_180x360_aave.20181001.nc"   # WARNING HARDCODED
        info_file = NamedTemporaryFile(delete=False)
        cmd = f"e3sm_to_cmip -i {parameters['mpas_data_path']} --info --realm mpaso --map {mapfile} -v {var_string} -t {self.config['cmip_tables_path']} --info-out {info_file.name}"
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

        std_var_list = []
        std_cmor_list = []

        parameters['std_var_list'] = ['PSL']    # for pbo, pso

        # Apply variable info to parameters collection
        for item in variable_info:
            if isinstance(item['E3SM Variables'], list):
                std_var_list.extend([v for v in item['E3SM Variables']])
            std_cmor_list.append(item['CMIP6 Name'])

        log_message("info", f"DBG: resolve_cmd: obtained std_cmor_list = {std_cmor_list}")

        # parameters['cmor_var_list'] = std_cmor_list
        parameters['cmor_var_list'] = cmip_var
        cwl_workflow = "mpaso/mpaso.cwl"

        parameters['tables_path'] = self.config['cmip_tables_path']
        # was 'metadata_path'
        parameters['metadata'] = {
            'class': 'File',
            'path': os.path.join(
                self.config['cmip_metadata_path'], 
                model_version, 
                f"{experiment}_{variant}.json")
            }
        # parameters['hrz_atm_map_path'] = self.config['grids']['ne30_to_180x360']
        parameters['mapfile'] = { 'class': 'File', 'path': self.config['grids']['oEC60to30_to_180x360'] }
        log_message("info", f"Applying to parameters[mapfile]: type = {type(parameters['mapfile'])}")

        raw_case_spec = self._spec['project']['E3SM'][raw_ocean_dataset.model_version][raw_ocean_dataset.experiment]

        #DEBUG
        for item in raw_case_spec:
            log_message("info", f"resolve_cmd: raw_case_spec[{item}] = {raw_case_spec[item]}")

        # parameters['mpas_namelist_path'] = raw_case_spec['mpaso_namelist']
        # parameters['mpas_restart_path'] = raw_case_spec['mpas_restart']
        parameters['namelist_path'] = "/p/user_pub/e3sm/staging/resource/namefiles/mpaso_in"                       # WARNING HARDCODED
        parameters['restart_path'] = "/p/user_pub/e3sm/staging/resource/restarts/mpaso.rst.1851-01-01_00000.nc"    # WARNING HARDCODED
        parameters['region_path'] = "/p/user_pub/e3sm/staging/resource/oEC60to30v3_Atlantic_region_and_southern_transect.nc"  # WARNING HARDCODED
        # parameters['mapfile'] = mapfile
        parameters['workflow_output'] = str(self.dataset.warehouse_path)
        parameters['frequency'] = 10

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
        self._cmd = f"cwltool --outdir {outpath} --tmpdir-prefix={self.tmpdir} {parallel} --preserve-environment UDUNITS2_XML_PATH {os.path.join(self.config['cwl_workflows_path'], cwl_workflow)} {parameter_path}"
