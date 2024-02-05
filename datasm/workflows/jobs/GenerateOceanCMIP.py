import os
from pathlib import Path

import yaml
from datasm.workflows.jobs import WorkflowJob
from datasm.util import log_message, get_e2c_info, parent_native_dsid, latest_data_vdir, prepare_cmip_job_metadata, derivative_conf, latest_aux_data

NAME = 'GenerateOceanCMIP'

oa_vars = ['pbo', 'pso']

class GenerateOceanCMIP(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        log_message("info", f"INIT: set self.name = {NAME} self.dataset.dataset_id = {self.dataset.dataset_id}")
        cmip_var = self.dataset.dataset_id.split('.')[-2]
        if cmip_var in oa_vars:
            self._requires = { 'ocean-native-mon': None, 'atmos-native-mon': None }
        else:
            self._requires = { 'ocean-native-mon': None }
        self._cmd = ''

    def resolve_cmd(self):

        target_dsid = self.dataset.dataset_id
        parent_dsid = parent_native_dsid(target_dsid)

        cwl_config = self.config['cmip_ocn_mon']
        metadata_path = self.config['cmip_metadata_path']
        resource_path = self.config['e3sm_resource_path']
        tables_path = self.config['cmip_tables_path']

        deriv_conf = derivative_conf(target_dsid, resource_path)

        ocn_data_path = latest_data_vdir(parent_dsid)
        outpath = self.config['DEFAULT_WAREHOUSE_PATH']

        metadata_file = prepare_cmip_job_metadata(target_dsid, metadata_path, self._slurm_out)

        _, _, institution, cmip_model_version, experiment, variant, table, cmip_var, _ = self.dataset.dataset_id.split('.')

        freq = parent_dsid.split('.')[7]
        realm = "ocn"

        # build up parameters list for job config .yaml write
        parameters = dict()

        parameters['tables_path'] = tables_path
        parameters['metadata'] = { 'class': 'File', 'path': metadata_file }

        is_oa_var = False
        if cmip_var in oa_vars:
            is_oa_var = True

        if is_oa_var:  # pbo, pso
            parameters['metadata_path'] = parameters['metadata']        # fix mpaso-atm to eliminate alternate name
            parameters['mpas_data_path'] = ocn_data_path
            parameters['atm_data_path'] = "" # placeholder
            parameters['std_var_list'] = ['PSL']
            parameters['mpas_var_list'] = [cmip_var]
            parameters['mpas_map_path'] = self.config['grids']['oEC60to30_to_180x360']  # (superfluous?)
            cwl_workflow_main = "mpaso-atm/mpaso-atm.cwl"
        else:
            parameters['data_path'] = ocn_data_path
            parameters['cmor_var_list'] = [cmip_var]
            cwl_workflow_main = "mpaso/mpaso.cwl"


        parameters.update(cwl_config)   # obtain frequency, num_workers, account, partition, e2c_timeout, slurm_timeout
        parameters.update(deriv_conf)   # obtain hrz_atm_map_path, mapfile, region_file, file_pattern, case_finder

        namefile = latest_aux_data(target_dsid, "namefile", False)
        restfile = latest_aux_data(target_dsid, "restart", True)
        if namefile == "NONE" or restfile == "NONE":
            log_message("error","Could not obtain namefile or restart file for job params")
            os._exit(1)

        parameters['namelist_path'] = namefile
        parameters['restart_path'] = restfile

        # Override default 10 YPF for certain variables
        if cmip_var in [ 'all', 'hfsifrazil', 'masscello', 'so', 'thetao', 'thkcello', 'uo', 'vo', 'volcello', 'wo', 'zhalfo' ]:
            parameters['frequency'] = 5

        # YPF to 500 for "fixed"
        if table == "Ofx":
            parameters['frequency'] = 500

        parameters['workflow_output'] = outpath

        # write out the parameter file and setup the temp directory
        parameters_name = f"{experiment}-{cmip_model_version}-{variant}-{realm}-cmip-{freq}-{cmip_var}.yaml"
        parameters_path = os.path.join(self._slurm_out, parameters_name)
        with open(parameters_path, 'w') as outstream:
            yaml.dump(parameters, outstream)

        cwl_workflow_path = os.path.join(self.config['cwl_workflows_path'], cwl_workflow_main)

        if not self.serial:
            parallel = "--parallel"
        else:
            parallel = ''
        self._cmd = f"cwltool --outdir {outpath} --tmpdir-prefix={self.tmpdir} {parallel} --preserve-environment UDUNITS2_XML_PATH {cwl_workflow_path} {parameters_path}"
