import os
from pathlib import Path
from subprocess import PIPE, Popen
from tempfile import NamedTemporaryFile

import yaml
from datasm.util import log_message, prepare_cmip_job_metadata, get_first_nc_file
from datasm.workflows.jobs import WorkflowJob

NAME = 'GenerateAtmFixedCMIP'


class GenerateAtmFixedCMIP(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = {'ocean-native-mon': None}
        self._cmd = ''
        self._cmip_var = ''

    def resolve_cmd(self):

        raw_dataset = self.requires['ocean-native-mon']

        data_path = raw_dataset.latest_warehouse_dir
        anyfile = get_first_nc_file(data_path)
        anypath = os.path.join(data_path, anyfile)
        data_path_dict = { 'class': 'File', 'path': anypath }

        parameters = {'ocn_data_path': data_path_dict }
        cwl_config = self.config['cmip_ocn_mon']
        parameters.update(cwl_config)   # obtain frequency, num_workers, account, partition, timeout, slurm_timeout

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
        cmd = f"e3sm_to_cmip --info -i {data_path} --freq mon -v {', '.join(in_cmip_vars)} -t {self.config['cmip_tables_path']} --info-out {info_file.name}"
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

        parameters['std_var_list'] = e3sm_vars
        parameters['cmor_var_list'] = cmip_vars

        log_message("info", f"Obtained e3sm_vars: {', '.join(e3sm_vars)}")
        log_message("info", f"Obtained cmip_vars: {', '.join(cmip_vars)}")

        # Obtain mapfile and region_file by model_version

        if model_version == "E3SM-2-0":
            parameters['mapfile'] = self.config['grids']['v2_oEC60to30_to_180x360']
            parameters['region_file'] = parameters['v2_mpas_region_path']
        else:
            parameters['mapfile'] = self.config['grids']['v1_oEC60to30_to_180x360']
            parameters['region_file'] = parameters['v1_mpas_region_path']

        parameters['tables_path'] = self.config['cmip_tables_path']

        parameters['metadata'] = prepare_cmip_job_metadata(self.dataset.dataset_id, self.config['cmip_metadata_path'], self._slurm_out)

        # step two, write out the parameter file and setup the temp directory
        var_id = 'all' if is_all else in_cmip_vars[0]
        parameter_path = os.path.join(
            self._slurm_out, f"{self.dataset.experiment}-{self.dataset.model_version}-{self.dataset.ensemble}-ocn-cmip-mon-{var_id}.yaml")
        with open(parameter_path, 'w') as outstream:
            yaml.dump(parameters, outstream)

        # step three, render out the CWL run command
        # OVERRIDE : needed to be "pub_dir" to find the data, but back to "warehouse" to write results to the warehouse
        outpath = '/p/user_pub/e3sm/warehouse'  # was "self.dataset.warehouse_base", but -w <pub_root> for input selection interferes.

        cwl_workflow_main = "Ofx/Ofx.cwl"
        cwl_workflow_path = os.path.join(self.config['cwl_workflows_path'], cwl_workflow_main)
        
        if not self.serial:
            parallel = "--parallel"
        else:
            parallel = ''
        self._cmd = f"cwltool --outdir {outpath} --tmpdir-prefix={self.tmpdir} {parallel} --preserve-environment UDUNITS2_XML_PATH {cwl_workflow_path} {parameter_path}"


#
# e2c command-line for Ofx generation:

# e3sm_to_cmip -s --realm Ofx --var-list areacello --map /p/user_pub/e3sm/staging/resource/maps/map_EC30to60E2r2_to_cmip6_180x360_aave.20220301.nc --input-path /p/user_pub/e3sm/zhang40/e2c_tony/e2c_test_data/v2.mpassi_input/ --output-path /p/user_pub/e3sm/zhang40/tests/ncremap_sgs  --user-metadata /p/user_pub/e3sm/zhang40/e2c_tony/e2c_test_data/holodeck/input/historical_r1i1p1f1.json --tables-path /p/user_pub/e3sm/staging/resource/cmor/cmip6-cmor-tables/Tables

#  

