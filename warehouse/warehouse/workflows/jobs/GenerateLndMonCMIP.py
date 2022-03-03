import yaml
import os
from pathlib import Path
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from warehouse.workflows.jobs import WorkflowJob
from warehouse.util import log_message

NAME = 'GenerateLndMonCMIP'


class GenerateLndMonCMIP(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = {'land-native-mon': None}
        self._cmd = ''

    def resolve_cmd(self):

        print("============================================================================================================================")
        print(f"self.dataset.data_path: {self.dataset.data_path}")
        # print(f"self.dataset.last_pdir: {self.dataset.latest_pub_dir}")
        # print(f"self.dataset.last_wdir: {self.dataset.latest_warehouse_dir}")
        print(f"self.dataset.pub_base:  {self.dataset.pub_base}")
        print(f"self.dataset.pub_path:  {self.dataset.publication_path}")
        print(f"self.dataset.wh_base:   {self.dataset.warehouse_base}")
        print(f"self.dataset.wh_path:   {self.dataset.warehouse_path}")
        print("============================================================================================================================")
        print(" ", flush=True)
        # all debugging
        # self._cmd = f"ls -l"
        # return
        # all debugging
        

        raw_dataset = self.requires['land-native-mon']
        if raw_dataset is None:
            log_message("error", f"Job {NAME} doesnt have its requirements filled: {self.requires}")
            raise ValueError(f"Job {NAME} doesnt have its requirements filled: {self.requires}")
        cwl_config = self.config['cmip_lnd_mon']

        parameters = {'lnd_data_path': raw_dataset.latest_warehouse_dir}
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

        std_var_list = []
        std_cmor_list = []
        info_file = NamedTemporaryFile(delete=False)
        cmd = f"e3sm_to_cmip -i {parameters['lnd_data_path']} --info --realm lnd -v {', '.join(cmip_var)} -t {self.config['cmip_tables_path']} --info-out {info_file.name}"
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

        # step three, render out the CWL run command
        # OVERRIDE : needed to be "pub_dir" to find the data, but back to "warehouse" to write results to the warehouse
        outpath = '/p/user_pub/e3sm/warehouse'  # was "self.dataset.warehouse_base", but -w <pub_root> for input selection interferes.
        
        log_message("info", f"DEBUG-001: render out the CWL run command: cwltool --outdir {outpath} --tmpdir-prefix={self.tmpdir} --preserve-environment UDUNITS2_XML_PATH {os.path.join(self.config['cwl_workflows_path'], cwl_workflow)} {parameter_path}")
        self._cmd = f"cwltool --outdir {outpath} --tmpdir-prefix={self.tmpdir} --preserve-environment UDUNITS2_XML_PATH {os.path.join(self.config['cwl_workflows_path'], cwl_workflow)} {parameter_path}"

