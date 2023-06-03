import os
import yaml
from pathlib import Path
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from datasm.workflows.jobs import WorkflowJob
from datasm.util import log_message, prepare_cmip_job_metadata, latest_aux_data, derivative_conf

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

        # Obtain metadata file, after move to self._slurm_out and current-date-based version edit

        parameters['metadata'] = prepare_cmip_job_metadata(self.dataset.dataset_id, self.config['cmip_metadata_path'], self._slurm_out)

        # Obtain latest data path

        parameters['data_path'] = raw_seaice_dataset.latest_warehouse_dir

        parameters.update(cwl_config)   # obtain frequency, num_workers, account, partition, timeout, slurm_timeout

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
        parameters['cmor_var_list'] = cmip_var

        # Obtain mapfile and region_file by model_version

        parameters.update( derivative_conf(self.dataset.dataset_id, self.config['e3sm_resource_path']) )

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

        cwl_workflow_main = "mpassi/mpassi.cwl"
        cwl_workflow_path = os.path.join(self.config['cwl_workflows_path'], cwl_workflow_main)

        if not self.serial:
            parallel = "--parallel"
        else:
            parallel = ''
        self._cmd = f"cwltool --outdir {outpath} --tmpdir-prefix={self.tmpdir} {parallel} --preserve-environment UDUNITS2_XML_PATH {cwl_workflow_path} {parameter_path}"
