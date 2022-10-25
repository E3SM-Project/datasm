import yaml
import os

from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from datasm.workflows.jobs import WorkflowJob
from datasm.util import log_message, get_UTC_YMD, set_version_in_user_metadata

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

        # seek "model_version" for v2 accommodations

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
        plev_var_list = []
        plev_cmor_list = []

        info_file = NamedTemporaryFile(delete=False)
        cmd = f"e3sm_to_cmip --info -i {parameters['data_path']} --freq mon -v {', '.join(cmip_var)} -t {self.config['cmip_tables_path']} --info-out {info_file.name}"
        proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        _, err = proc.communicate()
        if err:
            log_message("info", f"(stderr) checking variables: {err}")
            # return None # apparently not a serious error, merely data written to stderr.

        plev = False
        mlev = False
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

        parameters['vrt_map_path'] = self.config['vrt_map_path']        # not needed for mlev-only but no harm
        parameters['std_var_list'] = std_var_list                       # for mlev, else no harm if empty
        parameters['std_cmor_list'] = std_cmor_list                     # for mlev, else no harm if empty
        parameters['plev_var_list'] = plev_var_list                     # for plev, else no harm if empty
        parameters['plev_cmor_list'] = plev_cmor_list                   # for plev, else no harm if empty

        if model_version == "E3SM-2-0":
            parameters['hrz_atm_map_path'] = self.config['grids']['v2_ne30_to_180x360']
            if plev and not mlev:
                cwl_workflow = "atm-mon-plev-eam/atm-plev.cwl"
            elif not plev and mlev:
                cwl_workflow = "atm-mon-model-lev-eam/atm-std.cwl"
            elif plev and mlev:
                cwl_workflow = "atm-unified-eam/atm-unified.cwl"
        else:
            parameters['hrz_atm_map_path'] = self.config['grids']['v1_ne30_to_180x360']
            if plev and not mlev:
                cwl_workflow = "atm-mon-plev/atm-plev.cwl"
            elif not plev and mlev:
                cwl_workflow = "atm-mon-model-lev/atm-std.cwl"
            elif plev and mlev:
                cwl_workflow = "atm-unified/atm-unified.cwl"

        log_message("info", f"resolve_cmd: Employing cwl_workflow {cwl_workflow}")

        parameters['tables_path'] = self.config['cmip_tables_path']
        parameters['metadata_path'] = os.path.join(
            self.config['cmip_metadata_path'], model_version, f"{experiment}_{variant}.json")   # model_version = CMIP6 "Source"

        # force dataset output version here
        ds_version = "v" + get_UTC_YMD()
        set_version_in_user_metadata(parameters['metadata_path'], ds_version)
        log_message("info", f"Set dataset version in {parameters['metadata_path']} to {ds_version}")

        # step two, write out the parameter file and setup the temp directory
        self._cmip_var = 'all' if is_all else cmip_var[0]
        parameter_path = os.path.join(
            self._slurm_out, f"{self.dataset.experiment}-{self.dataset.model_version}-{self.dataset.ensemble}-atm-cmip-mon-{self._cmip_var}.yaml")
        with open(parameter_path, 'w') as outstream:
            yaml.dump(parameters, outstream)

        # step three, render out the CWL run command
        # OVERRIDE : needed to be "pub_dir" to find the data, but back to "warehouse" to write results to the warehouse
        self.dataset.warehouse_base = '/p/user_pub/e3sm/warehouse'      # testing testing testing ...

        if not self.serial:
            parallel = "--parallel"
        else:
            parallel = ''
        self._cmd = f"cwltool --outdir {self.dataset.warehouse_base}  --tmpdir-prefix={self.tmpdir} {parallel} --preserve-environment UDUNITS2_XML_PATH {os.path.join(self.config['cwl_workflows_path'], cwl_workflow)} {parameter_path}"