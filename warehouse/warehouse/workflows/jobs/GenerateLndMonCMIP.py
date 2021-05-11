import yaml
import os
from pathlib import Path
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from warehouse.workflows.jobs import WorkflowJob

NAME = 'GenerateLndMonCMIP'


class GenerateLndMonCMIP(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = {'land-native-mon': None}
        self._cmd = ''

    def resolve_cmd(self):

        # step one, collect the information we're going to need for the CWL parameter file
        with open(self._spec_path, 'r') as i:
            spec = yaml.load(i, Loader=yaml.SafeLoader)

        raw_dataset = self.requires['land-native-mon']
        if raw_dataset is None:
            raise ValueError(f"Job {NAME} doesnt have its requirements filled: {self.requires}")
        cwl_config = self.config['cmip_lnd_mon']

        parameters = {'lnd_data_path': raw_dataset.latest_warehouse_dir}
        parameters.update(cwl_config)

        _, _, _, model_version, experiment, variant, table, cmip_var, _ = self.dataset.dataset_id.split('.')
        
        # if we want to run all the variables
        # we can pull them from the dataset spec
        if cmip_var == 'all':
            is_all = True
            cmip_var = [x for x in spec['tables'][table] if x != 'all']
        else:
            is_all = False
            cmip_var = [cmip_var]

        std_var_list = []
        std_cmor_list = []
        info_file = NamedTemporaryFile(delete=False)
        cmd = f"e3sm_to_cmip -i {parameters['lnd_data_path']} --info --mode lnd -v {', '.join(cmip_var)} -t {self.config['cmip_tables_path']} --info-out {info_file.name}"
        proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        _, err = proc.communicate()
        if err:
            print(err)
            return None
    
        with open(info_file.name, 'r') as instream:
            variable_info = yaml.load(instream, Loader=yaml.SafeLoader)
        if variable_info is None:
            raise ValueError(f"Unable to find correct input variable for requested CMIP conversion, is {cmd} correct?")

        for item in variable_info:
            if ',' in item['E3SM Variables']:
                e3sm_var = [v.strip() for v in item['E3SM Variables'].split(',')]
            else:
                e3sm_var = [item['E3SM Variables']]

            std_var_list.extend(e3sm_var)
            std_cmor_list.append(item['CMIP6 Name'])


        parameters['lnd_var_list'] = std_var_list
        parameters['cmor_var_list'] = std_cmor_list
        parameters['one_land_file'] = os.path.join(
            parameters['lnd_data_path'],
            os.listdir(parameters['lnd_data_path']).pop())
        cwl_workflow = "lnd-n2n/lnd.cwl"


        parameters['tables_path'] = self.config['cmip_tables_path']
        parameters['metadata_path'] = os.path.join(
            self.config['cmip_metadata_path'], model_version, f"{experiment}_{variant}.json")
        parameters['hrz_atm_map_path'] = self.config['grids']['ne30_to_180x360']

        # step two, write out the parameter file and setup the temp directory
        var_id = 'all' if is_all else cmip_var[0]
        parameter_path = os.path.join(
            self._slurm_out, f"{self.dataset.experiment}-{self.dataset.model_version}-{self.dataset.ensemble}-lnd-cmip-mon-{var_id}.yaml")
        with open(parameter_path, 'w') as outstream:
            yaml.dump(parameters, outstream)

        tmp_path = Path(self._slurm_out, 'tmp')
        if not tmp_path.exists():
            tmp_path.mkdir()

        # step three, render out the CWL run command
        self._cmd = f"cwltool --tmpdir-prefix={tmp_path} --preserve-environment UDUNITS2_XML_PATH {os.path.join(self.config['cwl_workflows_path'], cwl_workflow)} {parameter_path}"
