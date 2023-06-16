import yaml
import os, sys

from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from datasm.workflows.jobs import WorkflowJob
from datasm.util import log_message, prepare_cmip_job_metadata, derivative_conf

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
        parameters.update(cwl_config)   # obtain frequency, num_workers, account, partition, e2c_timeout, slurm_timeout

        _, _, institution, cmip_model_version, experiment, variant, table, cmip_var, _ = self.dataset.dataset_id.split('.')

        # seek "cmip_model_version" for v2 accommodations

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

        # Obtain metadata file, after move to self._slurm_out and current-date-based version edit

        metadata_path = prepare_cmip_job_metadata(self.dataset.dataset_id, self.config['cmip_metadata_path'], self._slurm_out)
        parameters['metadata_path'] = metadata_path

        info_file = NamedTemporaryFile(delete=False)
        cmip_out = os.path.join(self._slurm_out, "CMIP6")
        cmd = f"e3sm_to_cmip --info -i {parameters['data_path']} -o {cmip_out} -u {metadata_path} --freq mon -v {', '.join(cmip_var)} -t {self.config['cmip_tables_path']} --info-out {info_file.name}"
        log_message("info", f"resolve_cmd: calling e2c: CMD = {cmd}")

        print(f"DEBUG: calling e2c: CMD = {cmd}", flush=True)

        proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        _, err = proc.communicate()
        if err:
            log_message("info", f"ERROR checking variables: {err}")

        plev = False
        mlev = False
        with open(info_file.name, 'r') as instream:
            variable_info = yaml.load(instream, Loader=yaml.SafeLoader)

        if variable_info == None:
            log_message("error", f"ERROR checking variables: No data returned from e3sm_to_cmip --info")
            os._exit(1)

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

        if not mlev and not plev:
            log_message("error", "resolve_cmd: e3sm_to_cmip --info returned EMPTY variable info")
            self._cmd = "echo EMPTY variable info; exit 1"
            return 1

        parameters['vrt_map_path'] = self.config['vrt_map_path']        # not needed for mlev-only but no harm
        parameters['std_var_list'] = std_var_list                       # for mlev, else no harm if empty
        parameters['std_cmor_list'] = std_cmor_list                     # for mlev, else no harm if empty
        parameters['plev_var_list'] = plev_var_list                     # for plev, else no harm if empty
        parameters['plev_cmor_list'] = plev_cmor_list                   # for plev, else no harm if empty

        parameters['tables_path'] = self.config['cmip_tables_path']

        parameters.update( derivative_conf(self.dataset.dataset_id, self.config['e3sm_resource_path']) )

        if cmip_model_version == "E3SM-2-0":
            if plev and not mlev:
                cwl_workflow_main = "atm-mon-plev/atm-plev.cwl"
            elif not plev and mlev:
                cwl_workflow_main = "atm-mon-model-lev/atm-std.cwl"
            elif plev and mlev:
                cwl_workflow_main = "atm-unified-eam/atm-unified.cwl"
        else:
            if plev and not mlev:
                cwl_workflow_main = "atm-mon-plev/atm-plev.cwl"
            elif not plev and mlev:
                cwl_workflow_main = "atm-mon-model-lev/atm-std.cwl"
            elif plev and mlev:
                cwl_workflow_main = "atm-unified/atm-unified.cwl"

        cwl_workflow_path = os.path.join(self.config['cwl_workflows_path'], cwl_workflow_main)

        # step two, write out the parameter file and setup the temp directory
        self._cmip_var = 'all' if is_all else cmip_var[0]
        parameter_path = os.path.join(
            self._slurm_out, f"{self.dataset.experiment}-{self.dataset.model_version}-{self.dataset.ensemble}-atm-cmip-mon-{self._cmip_var}.yaml")
        with open(parameter_path, 'w') as outstream:
            yaml.dump(parameters, outstream)

        # step three, render out the CWL run command
        # OVERRIDE : needed to be "pub_dir" to find the data, but back to "warehouse" to write results to the warehouse
        outpath = self.config['DEFAULT_WAREHOUSE_PATH']  # was "self.dataset.warehouse_base", but -w <pub_root> for input selection interferes.

        if not self.serial:
            parallel = "--parallel"
        else:
            parallel = ''
        self._cmd = f"cwltool --outdir {outpath} --tmpdir-prefix={self.tmpdir} {parallel} --preserve-environment UDUNITS2_XML_PATH {cwl_workflow_path} {parameter_path}"
