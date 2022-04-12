import yaml
import os
import string
from pathlib import Path
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from warehouse.workflows.jobs import WorkflowJob
from warehouse.util import log_message

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

        # log_message("debug", f"Using raw input from {raw_dataset.latest_warehouse_dir}")
        parameters = {'data_path': raw_dataset.latest_warehouse_dir}
        parameters.update(cwl_config)

        _, _, _, model_version, experiment, variant, table, cmip_var, _ = self.dataset.dataset_id.split('.')
        
        # if we want to run all the variables
        # we can pull them from the dataset spec
        if cmip_var == 'all':
            is_all = True
            cmip_vars = [x for x in self._spec['tables'][table] if x != 'all']
        else:
            is_all = False
            cmip_vars = [cmip_var]

        info_file = NamedTemporaryFile(delete=False)
        cmd = f"e3sm_to_cmip --info -i {parameters['data_path']} --freq day -v {', '.join(cmip_vars)} -t {self.config['cmip_tables_path']} --info-out {info_file.name}"
        log_message("info", f"resolve_cmd: issuing info cmd: {cmd}")

        proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        _, err = proc.communicate()
        if err:
            log_message("debug", f"Error checking variables: {err}")
            return None
        # else:
        #     log_message("debug", "e3sm_to_cmip returned variable info")
    
        with open(info_file.name, 'r') as instream:
            variable_info = yaml.load(instream, Loader=yaml.SafeLoader)

        # the high freq variable handler may have a different
        # name then the actual CMIP6 variable, for example
        # the daily pr handler is named pr_highfreq
        e3sm_vars = []
        real_cmip_vars = []
        for item in variable_info:
            if isinstance(item['E3SM Variables'], list):
                e3sm_vars.extend([v for v in item['E3SM Variables']])
            else:
                e3sm_vars.append(item['E3SM Variables'])

            vname = item['CMIP6 Name']
            if vname in ['pr', 'rlut']:
                vname = f"{vname}_highfreq" # only for this day or 3hr job

            real_cmip_vars.append(vname)

        parameters['std_var_list'] = e3sm_vars
        parameters['std_cmor_list'] = real_cmip_vars
        
        cwl_workflow = "atm-highfreq/atm-highfreq.cwl"
        parameters['tables_path'] = self.config['cmip_tables_path']
        parameters['metadata_path'] = os.path.join(
            self.config['cmip_metadata_path'], model_version, f"{experiment}_{variant}.json")
        parameters['hrz_atm_map_path'] = self.config['grids']['ne30_to_180x360']

        # step two, write out the parameter file and setup the temp directory
        var_id = 'all' if is_all else cmip_vars[0]
        parameter_path = os.path.join(
            self._slurm_out, f"{self.dataset.experiment}-{self.dataset.model_version}-{self.dataset.ensemble}-atm-cmip-day-{var_id}.yaml")
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
