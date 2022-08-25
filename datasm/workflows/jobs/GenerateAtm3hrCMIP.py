import os
from pathlib import Path
from subprocess import PIPE, Popen
from tempfile import NamedTemporaryFile

import yaml
from datasm.util import log_message, get_UTC_YMD, set_version_in_user_metadata
from datasm.workflows.jobs import WorkflowJob

NAME = 'GenerateAtm3hrCMIP'


class GenerateAtm3hrCMIP(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = {'atmos-native-3hr': None}
        self._cmd = ''
        self._cmip_var = ''

    def resolve_cmd(self):

        raw_dataset = self.requires['atmos-native-3hr']
        cwl_config = self.config['cmip_atm_3hr']

        # log_message("debug", f"Using raw input from {raw_dataset.latest_warehouse_dir}")
        parameters = {'data_path': raw_dataset.latest_warehouse_dir}
        parameters.update(cwl_config)

        _, _, _, model_version, experiment, variant, table, cmip_var, _ = self.dataset.dataset_id.split('.')

        # if we want to run all the variables
        # we can pull them from the dataset spec
        if cmip_var == 'all':
            is_all = True
            in_cmip_vars = [x for x in self._spec['tables'][table] if x != 'all']
        else:
            is_all = False
            in_cmip_vars = [cmip_var]

        info_file = NamedTemporaryFile(delete=False)
        log_message("info", f"Obtained temp info file name: {info_file.name}")
        cmd = f"e3sm_to_cmip --info -i {parameters['data_path']} --freq 3hr -v {', '.join(in_cmip_vars)} -t {self.config['cmip_tables_path']} --info-out {info_file.name}"
        log_message("info", f"resolve_cmd: issuing variable info cmd: {cmd}")

        proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        _, err = proc.communicate()
        if err:
            log_message("info", f"(stderr) checking variables: {err}")
            # return None # apparently not a serious error, merely data written to stderr.

        with open(info_file.name, 'r') as instream:
            variable_info = yaml.load(instream, Loader=yaml.SafeLoader)

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
        if len(cmip_vars) == 0:
            log_message("info", "error: no cmip_vars identified")
            return None

        log_message("info", f"Obtained e3sm_vars: {', '.join(e3sm_vars)}")
        log_message("info", f"Obtained cmip_vars: {', '.join(cmip_vars)}")

        parameters['std_var_list'] = e3sm_vars
        parameters['std_cmor_list'] = cmip_vars

        cwl_workflow = "atm-highfreq/atm-highfreq.cwl"
        parameters['tables_path'] = self.config['cmip_tables_path']
        parameters['metadata_path'] = os.path.join(self.config['cmip_metadata_path'], model_version, f"{experiment}_{variant}.json")
        parameters['hrz_atm_map_path'] = self.config['grids']['ne30_to_180x360']

        # force dataset output version here
        ds_version = "v" + get_UTC_YMD()
        set_version_in_user_metadata(parameters['metadata_path'], ds_version)
        log_message("info", f"Set dataset version in {parameters['metadata_path']} to {ds_version}")

        # step two, write out the parameter file and setup the temp directory
        var_id = 'all' if is_all else in_cmip_vars[0]
        parameter_path = os.path.join(
            self._slurm_out, f"{self.dataset.experiment}-{self.dataset.model_version}-{self.dataset.ensemble}-atm-cmip-3hr-{var_id}.yaml")
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
