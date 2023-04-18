import os
import yaml
from pathlib import Path
from subprocess import PIPE, Popen
from tempfile import NamedTemporaryFile
from datasm.workflows.jobs import WorkflowJob
from datasm.util import log_message, prepare_cmip_job_metadata, latest_aux_data, derivative_conf

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

        # Begin parameters collection

        parameters = dict()
        parameters.update(cwl_config)   # obtain up frequency, num_workers, account, partition, e2c_timeout, slurm_timeout

        # start with universal constants

        parameters['tables_path'] = self.config['cmip_tables_path']

        # Obtain latest data path 

        # use 'mpas_data_path' and 'atm_data_path' for mpaso-atm.
        # including atmos-native_mon as (pbo requires PSL, etc)
        if is_oa_var:
            raw_ocean_dataset = self.requires['ocean-native-mon']
            raw_atmos_dataset = self.requires['atmos-native-mon']
            parameters['mpas_data_path'] = raw_ocean_dataset.latest_warehouse_dir
            parameters['atm_data_path'] = raw_atmos_dataset.latest_warehouse_dir
        else:
            raw_ocean_dataset = self.requires['ocean-native-mon']
            parameters['data_path'] = raw_ocean_dataset.latest_warehouse_dir

        # Override default 10 YPF for certain variables
        if cmip_var in [ 'all', 'hfsifrazil', 'masscello', 'so', 'thetao', 'thkcello', 'uo', 'vo', 'volcello', 'wo', 'zhalfo' ]:
            parameters['frequency'] = 5

        log_message("info", f"DBG: parameters['data_path'] = {parameters['data_path']}")

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


        # Apply variable info to parameters collection

        if is_oa_var:
            parameters['std_var_list'] = ['PSL']
            parameters['mpas_var_list'] = cmip_var
            cwl_workflow_main = "mpaso-atm/mpaso-atm.cwl"
        else:
            parameters['cmor_var_list'] = cmip_var
            cwl_workflow_main = "mpaso/mpaso.cwl"

        cwl_workflow_path = os.path.join(self.config['cwl_workflows_path'], cwl_workflow_main)

        # Obtain mapfile and region_file by model_version

        parameters.update( derivative_conf(self.dataset.dataset_id, self.config['e3sm_resource_path']) )

        if is_oa_var:
            parameters['mpas_map_path'] = self.config['grids']['oEC60to30_to_180x360']

        # Obtain metadata file, after move to self._slurm_out and current-date-based version edit

        parameters['metadata'] = prepare_cmip_job_metadata(self.dataset.dataset_id, self.config['cmip_metadata_path'], self._slurm_out)

        # if is_oa_var:
        #     parameters['metadata_path'] = parameters['metadata']

        namefile = latest_aux_data(self.dataset.dataset_id, "namefile", False)
        restfile = latest_aux_data(self.dataset.dataset_id, "restart", True)
        if namefile == "NONE" or restfile == "NONE":
            log_message("error","Could not obtain namefile or restart file for job params")
             
        parameters['namelist_path'] = namefile
        parameters['restart_path'] = restfile

        parameters['workflow_output'] = self.config['DEFAULT_WAREHOUSE_PATH']

        # step two, write out the parameter file and setup the temp directory
        var_id = 'all' if is_all else cmip_var[0]
        parameter_path = os.path.join(
            self._slurm_out, f"{self.dataset.experiment}-{self.dataset.model_version}-{self.dataset.ensemble}-ocn-cmip-mon-{var_id}.yaml")
        with open(parameter_path, 'w') as outstream:
            yaml.dump(parameters, outstream)

        # step three, render out the CWL run command
        # OVERRIDE : needed to be "pub_dir" to find the data, but back to "warehouse" to write results to the warehouse
        outpath = self.config['DEFAULT_WAREHOUSE_PATH']  # was "self.dataset.warehouse_base", but -w <pub_root> for input selection interferes.

        if not self.serial:
            parallel = "--parallel"
        else:
            parallel = ''
        self._cmd = f"cwltool --outdir {outpath} --tmpdir-prefix={self.tmpdir} {parallel} --preserve-environment UDUNITS2_XML_PATH {cwl_workflow_path} {parameter_path}"
