import yaml
import os
import string
from pathlib import Path
from subprocess import Popen, PIPE
from warehouse.workflows.jobs import WorkflowJob

NAME = 'GenerateAtmDayCMIP'


class GenerateAtmDayCMIP(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = {'atmos-native-day': None}
        self._cmd = ''

    def resolve_cmd(self):

        # step one, collect the information we're going to need for the CWL parameter file
        with open(self._spec_path, 'r') as i:
            spec = yaml.load(i, Loader=yaml.SafeLoader)

        raw_dataset = self.requires['atmos-native-day']
        cwl_config = self.config['cmip_atm_day']

        parameters = {'data_path': raw_dataset.latest_warehouse_dir}
        parameters.update(cwl_config)

        _, _, _, model_version, experiment, variant, table, cmip_var, _ = self.dataset.dataset_id.split('.')
        
        # if we want to run all the variables
        # we can pull them from the dataset spec
        if cmip_var == 'all':
            is_all = True
            cmip_var = [x for x in spec['tables'][table] if x != 'all']
        else:
            is_all = True
            cmip_var = [cmip_var]

        e3sm_vars = []
        cmd = f"e3sm_to_cmip --info --freq day -v {', '.join(cmip_var)} -t {self.config['cmip_tables_path']}"
        proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()
        if err:
            print(err)
            return None
        for line in out.decode('utf-8').splitlines():

            if 'E3SM Variables' not in line:
                continue
            for var in line.strip().split(':')[1].split(','):
                i = 0
                for letter in var:
                    if letter not in string.printable:
                        break
                    i += 1
                variable = var[:i].strip()
                e3sm_vars.append(variable)

        parameters['std_var_list'] = e3sm_vars
        parameters['std_cmor_list'] = cmip_var
        
        cwl_workflow = "atm-highfreq/atm-highfreq.cwl"
        parameters['tables_path'] = self.config['cmip_tables_path']
        parameters['metadata_path'] = os.path.join(
            self.config['cmip_metadata_path'], model_version, f"{experiment}_{variant}.json")
        parameters['hrz_atm_map_path'] = self.config['grids']['ne30_to_180x360']

        # step two, write out the parameter file and setup the temp directory
        var_id = 'all' if is_all else cmip_var[0]
        parameter_path = os.path.join(
            self._slurm_out, f"{self.dataset.experiment}-{self.dataset.model_version}-{self.dataset.ensemble}-atm-cmip-mon-{var_id}.yaml")
        with open(parameter_path, 'w') as outstream:
            yaml.dump(parameters, outstream)

        tmp_path = Path(self._slurm_out, 'tmp')
        if not tmp_path.exists():
            tmp_path.mkdir()

        # step three, render out the CWL run command
        self._cmd = f"cwltool --tmpdir-prefix={tmp_path} --outdir {self.dataset.warehouse_base} --preserve-environment UDUNITS2_XML_PATH {os.path.join(self.config['cwl_workflows_path'], cwl_workflow)} {parameter_path}"
