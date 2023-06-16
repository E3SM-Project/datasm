import yaml
import os
from pathlib import Path
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from datasm.util import log_message, prepare_cmip_job_metadata, derivative_conf
from datasm.workflows.jobs import WorkflowJob

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

        cwl_config = self.config['cmip_lnd_mon']

        _, _, institution, model_version, experiment, variant, table, cmip_var, _ = self.dataset.dataset_id.split('.')

        raw_dataset = self.requires['land-native-mon']
        if raw_dataset is None:
            log_message("error", f"Job {NAME} doesnt have its requirements filled: {self.requires}")
            raise ValueError(f"Job {NAME} doesnt have its requirements filled: {self.requires}")

        # Begin parameters collection

        parameters = dict()

        # Start with universal constants

        parameters['tables_path'] = self.config['cmip_tables_path']

        # Obtain latest data path

        parameters['data_path'] = raw_dataset.latest_warehouse_dir

        parameters.update(cwl_config)   # obtain frequency, num_workers, account, partition, e2c_timeout, slurm_timeout

        # if we want to run all the variables
        # we can pull them from the dataset spec
        if cmip_var == 'all':
            is_all = True
            cmip_var = [x for x in self._spec['tables'][table] if x != 'all']
        else:
            is_all = False
            cmip_var = [cmip_var]

        # Call E2C to obtain variable info
        std_var_list = []
        std_cmor_list = []

        # Obtain metadata file, after move to self._slurm_out and current-date-based version edit

        metadata_path = prepare_cmip_job_metadata(self.dataset.dataset_id, self.config['cmip_metadata_path'], self._slurm_out)
        parameters['metadata_path'] = metadata_path

        info_file = NamedTemporaryFile(delete=False)
        cmip_out = os.path.join(self._slurm_out, "CMIP6")
        cmd = f"e3sm_to_cmip -i {parameters['data_path']} -o {cmip_out} -u {metadata_path} --info --map none --realm lnd -v {', '.join(cmip_var)} -t {self.config['cmip_tables_path']} --info-out {info_file.name}"
        log_message("info", f"resolve_cmd: E2C --info call cmd = {cmd}")
        proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        _, err = proc.communicate()
        if err:
            log_message("info", f"resolve_cmd: e3sm_to_cmip info {info_file.name} returned stderr data")
            log_message("info", f"  err = {err}")
            # return 1    # was return None

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

            std_var_list.extend(e3sm_var)
            std_cmor_list.append(item['CMIP6 Name'])

        # Apply variable info to parameters collection
        parameters['lnd_var_list'] = std_var_list
        parameters['cmor_var_list'] = std_cmor_list

        # Obtain file match pattern and mapfile by model_version

        parameters.update( derivative_conf(self.dataset.dataset_id, self.config['e3sm_resource_path']) )

        # step two, write out the parameter file and setup the temp directory
        var_id = 'all' if is_all else cmip_var[0]
        parameter_path = os.path.join(
            self._slurm_out, f"{self.dataset.experiment}-{self.dataset.model_version}-{self.dataset.ensemble}-lnd-cmip-mon-{var_id}.yaml")
        with open(parameter_path, 'w') as outstream:
            yaml.dump(parameters, outstream)

        if not self.serial:
            parallel = "--parallel"
        else:
            parallel = ''

        # step three, render out the CWL run command
        # OVERRIDE : needed to be "pub_dir" to find the data, but back to "warehouse" to write results to the warehouse
        outpath = self.config['DEFAULT_WAREHOUSE_PATH']  # was "self.dataset.warehouse_base", but -w <pub_root> for input selection interferes.

        cwl_workflow_main = "lnd-elm/lnd.cwl"
        cwl_workflow_path = os.path.join(self.config['cwl_workflows_path'], cwl_workflow_main)

        self._cmd = f"cwltool --outdir {outpath} --tmpdir-prefix={self.tmpdir} {parallel} --preserve-environment UDUNITS2_XML_PATH {cwl_workflow_path} {parameter_path}"

