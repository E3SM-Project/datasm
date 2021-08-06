import yaml
import os
import string
from pathlib import Path
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from warehouse.workflows.jobs import WorkflowJob

NAME = 'GenerateAtmMonCMIP'


class GenerateAtmMonCMIP(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = {'atmos-native-mon': None}
        self._cmd = ''
        self._cmip_var = ''

    def resolve_cmd(self):

        raw_dataset = self.requires['atmos-native-mon']
        cwl_config = self.config['cmip_atm_mon']

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

        std_var_list = []
        std_cmor_list = []
        plev_cmor_list = []
        plev_var_list = []
        plev = False
        mlev = False
        info_file = NamedTemporaryFile(delete=False)
        cmd = f"e3sm_to_cmip --info -i {parameters['data_path']} --freq mon -v {', '.join(cmip_var)} -t {self.config['cmip_tables_path']} --info-out {info_file.name}"
        proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        _, err = proc.communicate()
        if err:
            print(cmd)
            print(err)
            return None
    
        with open(info_file.name, 'r') as instream:
            variable_info = yaml.load(instream, Loader=yaml.SafeLoader)
        for item in variable_info:
            if ',' in item['E3SM Variables']:
                e3sm_var = [v.strip() for v in item['E3SM Variables'].split(',')]
            else:
                e3sm_var = [item['E3SM Variables']]

            if 'Levels' in item.keys() and item['Levels']['name'] == 'plev19':
                plev = True
                plev_var_list.extend(e3sm_var)
                plev_cmor_list.append(item['CMIP6 Name'])
            else:
                mlev = True
                std_var_list.extend(e3sm_var)
                std_cmor_list.append(item['CMIP6 Name'])

        if plev and not mlev:
            parameters['plev_var_list'] = plev_var_list
            parameters['plev_cmor_list'] = plev_cmor_list
            parameters['vrt_map_path'] = self.config['vrt_map_path']
            cwl_workflow = "atm-mon-plev/atm-plev.cwl"
        if not plev and mlev:
            parameters['std_var_list'] = std_var_list
            parameters['std_cmor_list'] = std_cmor_list
            cwl_workflow = "atm-mon-model-lev/atm-std.cwl"
        if plev and mlev:
            parameters['plev_var_list'] = plev_var_list
            parameters['plev_cmor_list'] = plev_cmor_list
            
            parameters['std_var_list'] = std_var_list
            parameters['std_cmor_list'] = std_cmor_list
            
            parameters['vrt_map_path'] = self.config['vrt_map_path']
            cwl_workflow = "atm-unified/atm-unified.cwl"
        try:
            a = cwl_workflow
        except:
            print(self.dataset.dataset_id)
            import ipdb; ipdb.set_trace()

        parameters['tables_path'] = self.config['cmip_tables_path']
        parameters['metadata_path'] = os.path.join(
            self.config['cmip_metadata_path'], model_version, f"{experiment}_{variant}.json")
        parameters['hrz_atm_map_path'] = self.config['grids']['ne30_to_180x360']

        # step two, write out the parameter file and setup the temp directory
        self._cmip_var = 'all' if is_all else cmip_var[0]
        parameter_path = os.path.join(
            self._slurm_out, f"{self.dataset.experiment}-{self.dataset.model_version}-{self.dataset.ensemble}-atm-cmip-mon-{self._cmip_var}.yaml")
        with open(parameter_path, 'w') as outstream:
            yaml.dump(parameters, outstream)

        # step three, render out the CWL run command
        if not self.serial:
            parallel = "--parallel"
        else:
            parallel = ''
        self._cmd = f"cwltool --outdir {self.dataset.warehouse_base}  --tmpdir-prefix={self.tmpdir} {parallel} --preserve-environment UDUNITS2_XML_PATH {os.path.join(self.config['cwl_workflows_path'], cwl_workflow)} {parameter_path}"
