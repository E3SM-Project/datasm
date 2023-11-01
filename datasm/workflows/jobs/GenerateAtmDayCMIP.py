import os
from pathlib import Path
from subprocess import PIPE, Popen
from tempfile import NamedTemporaryFile

import yaml
from datasm.util import log_message, prepare_cmip_job_metadata, derivative_conf
from datasm.workflows.jobs import WorkflowJob

NAME = 'GenerateAtmDayCMIP'


class GenerateAtmDayCMIP(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = {'atmos-native-day': None}
        self._cmd = ''
        self._cmip_var = ''

    def resolve_cmd(self):

        raw_dataset = self.requires['atmos-native-day']
        cwl_config = self.config['cmip_atm_day']

        _, _, institution, cmip_model_version, experiment, variant, table, cmip_var, _ = self.dataset.dataset_id.split('.')

        parameters = dict()
        parameters.update(cwl_config)   # obtain frequency, num_workers, account, partition, e2c_timeout, slurm_timeout

        data_path = raw_dataset.latest_warehouse_dir
        parameters['data_path'] = data_path

        # if we want to run all the variables
        # we can pull them from the dataset spec
        if cmip_var == 'all':
            is_all = True
            in_cmip_vars = [x for x in self._spec['tables'][table] if x != 'all']
        else:
            is_all = False
            in_cmip_vars = [cmip_var]

        # Obtain metadata file, after move to self._slurm_out and current-date-based version edit

        metadata_path = prepare_cmip_job_metadata(self.dataset.dataset_id, self.config['cmip_metadata_path'], self._slurm_out)
        parameters['metadata_path'] = metadata_path

        info_file = NamedTemporaryFile(delete=False)
        log_message("info", f"Obtained temp info file name: {info_file.name}")
        cmip_out = os.path.join(self._slurm_out, "CMIP6")
        var_str = ', '.join(in_cmip_vars)
        freq = "day"
        cmd = f"e3sm_to_cmip --info --map none -i {data_path} -o {cmip_out} -u {metadata_path} --freq {freq} -v {var_str} -t {self.config['cmip_tables_path']} --info-out {info_file.name} --realm atm"
        log_message("info", f"resolve_cmd: issuing variable info cmd: {cmd}")

        proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        _, err = proc.communicate()
        if err:
            log_message("info", f"(stderr) checking variables: {err}")
            # return None # apparently not a serious error, merely data written to stderr.

        with open(info_file.name, 'r') as instream:
            variable_info = yaml.load(instream, Loader=yaml.SafeLoader)

        if variable_info == None:
            log_message("error", f"ERROR checking variables: No data returned from e3sm_to_cmip --info: {cmd}")
            os._exit(1)

        e3sm_vars = []
        cmip_vars = []
        for item in variable_info:
            if isinstance(item['E3SM Variables'], list):
                e3sm_vars.extend([v for v in item['E3SM Variables']])
            else:
                e3sm_vars.append(item['E3SM Variables'])

            vname = item['CMIP6 Name']
            cmip_vars.append(vname)

        if len(e3sm_vars) == 0:
            log_message("info", "warning: no e3sm_vars identified")
            os._exit(1)
        if len(cmip_vars) == 0:
            log_message("info", "error: no cmip_vars identified")
            os._exit(1)

        log_message("info", f"Obtained e3sm_vars: {', '.join(e3sm_vars)}")
        log_message("info", f"Obtained cmip_vars: {', '.join(cmip_vars)}")

        parameters['std_var_list'] = e3sm_vars
        parameters['std_cmor_list'] = cmip_vars

        parameters['tables_path'] = self.config['cmip_tables_path']

        parameters.update( derivative_conf(self.dataset.dataset_id, self.config['e3sm_resource_path']) )

        # step two, write out the parameter file and setup the temp directory
        var_id = 'all' if is_all else in_cmip_vars[0]
        parameter_path = os.path.join(
            self._slurm_out, f"{self.dataset.experiment}-{self.dataset.model_version}-{self.dataset.ensemble}-atm-cmip-{freq}-{var_id}.yaml")
        with open(parameter_path, 'w') as outstream:
            yaml.dump(parameters, outstream)

        # step three, render out the CWL run command
        # OVERRIDE : needed to be "pub_dir" to find the data, but back to "warehouse" to write results to the warehouse
        outpath = self.config['DEFAULT_WAREHOUSE_PATH']  # was "self.dataset.warehouse_base", but -w <pub_root> for input selection interferes.

        cwl_workflow_main = "atm-highfreq/atm-highfreq.cwl"
        cwl_workflow_path = os.path.join(self.config['cwl_workflows_path'], cwl_workflow_main)

        if not self.serial:
            parallel = "--parallel"
        else:
            parallel = ''
        self._cmd = f"cwltool --outdir {outpath} --tmpdir-prefix={self.tmpdir} {parallel} --preserve-environment UDUNITS2_XML_PATH {cwl_workflow_path} {parameter_path}"
