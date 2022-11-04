import os
import yaml
import shutil
from pathlib import Path
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from datasm.workflows.jobs import WorkflowJob
from datasm.util import log_message, get_UTC_YMD, set_version_in_user_metadata, latest_aux_data

NAME = 'GenerateSeaIceCMIP'

class GenerateSeaIceCMIP(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = { 'seaice-native-mon': None, }
        self._cmd = ''

    def resolve_cmd(self):

        log_message("info", f"resolve_cmd: Start: dsid={self.dataset.dataset_id}")

        cwl_config = self.config['cmip_ocn_mon']

        _, _, _, model_version, experiment, variant, table, cmip_var, _ = self.dataset.dataset_id.split('.')

        raw_seaice_dataset = self.requires['seaice-native-mon']

        # Begin parameters collection

        parameters = dict()

        # Start with universal constants

        parameters['tables_path'] = self.config['cmip_tables_path']

        # Obtain latest data path

        parameters['data_path'] = raw_seaice_dataset.latest_warehouse_dir

        parameters.update(cwl_config)   # obtain frequency, num_workers, account, partition, timeout, slurm_timeout, mpas_region_path

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
        cmd = f"e3sm_to_cmip --info --realm mpassi --map dummy -v {var_string} -t {self.config['cmip_tables_path']} --info-out {info_file.name}"
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

        std_var_list = []
        std_cmor_list = []

        log_message("info", "==================================================================================")
        for item in variable_info:
            log_message("info", f"DBG: E2C INFO item = {item}")
        log_message("info", "==================================================================================")

        for item in variable_info:
            if isinstance(item['E3SM Variables'], list):
                std_var_list.extend([v for v in item['E3SM Variables']])
            std_cmor_list.append(item['CMIP6 Name'])

        log_message("info", f"DBG: resolve_cmd: obtained std_cmor_list = {std_cmor_list}")
        '''

        # Apply variable info to parameters collection
        parameters['cmor_var_list'] = cmip_var

        # assign cwl workflow

        cwl_workflow_main = "mpassi/mpassi.cwl"
        cwl_workflow = os.path.join(self.config['cwl_workflows_path'], cwl_workflow_main)
        log_message("info", f"resolve_cmd: Employing cwl_workflow {cwl_workflow}")

        # Obtain metadata file, move to self._slurm_out for current-date-based version edit

        metadata_name = f"{experiment}_{variant}.json"
        metadata_path_src = os.path.join(self.config['cmip_metadata_path'],model_version,f"{metadata_name}")
        shutil.copy(metadata_path_src,self._slurm_out)
        metadata_path =  os.path.realpath(os.path.join(self._slurm_out,metadata_name))
        # force dataset output version here
        ds_version = "v" + get_UTC_YMD()
        set_version_in_user_metadata(metadata_path, ds_version)
        log_message("info", f"Set dataset version in {metadata_path} to {ds_version}")
        parameters['metadata'] = { 'class': 'File', 'path': metadata_path }

        # Obtain mapfile and region_path by model_version

        if model_version == "E3SM-2-0":
            parameters['mapfile'] = { 'class': 'File', 'path': self.config['grids']['v2_oEC60to30_to_180x360'] }
            parameters['region_path'] = parameters['v2_mpas_region_path']
        else:
            parameters['mapfile'] = { 'class': 'File', 'path': self.config['grids']['v1_oEC60to30_to_180x360'] }
            parameters['region_path'] = parameters['v2_mpas_region_path']

        namefile = latest_aux_data(self.dataset.dataset_id, "namefile", False)
        restfile = latest_aux_data(self.dataset.dataset_id, "restart", True)
        if namefile == "NONE" or restfile == "NONE":
            log_message("error","Could not obtain namefile or restart file for job params")

        parameters['namelist_path'] = namefile
        parameters['restart_path'] = restfile

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
