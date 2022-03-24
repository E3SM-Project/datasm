import yaml
import os
from pathlib import Path
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from termcolor import cprint
from warehouse.workflows.jobs import WorkflowJob
from warehouse.util import log_message

NAME = 'GenerateAtm3hrCMIP'


class GenerateAtm3hrCMIP(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = {'atmos-native-3hr': None}
        self._cmd = ''

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
            cmip_var = [x for x in self._spec['tables'][table] if x != 'all']
        else:
            is_all = False
            cmip_var = [cmip_var]

        # need to ADD "_high_freq" for selected variables, crufty but necessary to not pollute the dataset_spec
        # only for the day and 3hr jobs.
        list1 = [ f"{x}_highfreq" for x in cmip_var if x in ['pr', 'rlut'] ]
        list2 = [ x for x in  cmip_var if x not in ['pr', 'rlut'] ]
        cmip_vars = list1 + list2

        info_file = NamedTemporaryFile(delete=False)
        cmd = f"e3sm_to_cmip --info -i {parameters['data_path']} --freq 3hr -v {', '.join(cmip_vars)} -t {self.config['cmip_tables_path']} --info-out {info_file.name}"
        # log_message("debug", f"resolve_cmd: Using e3sm_to_cmip to check for available variables: {cmd}")
        proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        _, err = proc.communicate()
        if err:
            log_message("debug", f"Error checking variables")
            cprint(err.decode('utf-8'), 'red')
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
                vname = f"{vname}_highfreq" # only for the day and 3hr jobs

            real_cmip_vars.append(vname)
        
        # import ipdb; ipdb.set_trace()
        # log_message("debug", f"Found the following E3SM variable to use as input {', '.join(e3sm_vars)}")

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
