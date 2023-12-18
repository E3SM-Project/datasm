import os
from pathlib import Path

import yaml
from datasm.workflows.jobs import WorkflowJob
from datasm.util import log_message, get_e2c_info, parent_native_dsid, latest_data_vdir, prepare_cmip_job_metadata, derivative_conf

NAME = 'GenerateAtmMonCMIP'


class GenerateAtmMonCMIP(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = {'atmos-native-mon': None}
        self._cmd = ''

    def resolve_cmd(self):

        target_dsid = self.dataset.dataset_id
        parent_dsid = parent_native_dsid(target_dsid)

        cwl_config = self.config['cmip_atm_mon']
        metadata_path = self.config['cmip_metadata_path']
        resource_path = self.config['e3sm_resource_path']
        tables_path = self.config['cmip_tables_path']

        deriv_conf = derivative_conf(target_dsid, resource_path)

        data_path = latest_data_vdir(parent_dsid)
        outpath = self.config['DEFAULT_WAREHOUSE_PATH']

        metadata_file = prepare_cmip_job_metadata(target_dsid, metadata_path, self._slurm_out)

        _, _, institution, cmip_model_version, experiment, variant, table, cmip_var, _ = self.dataset.dataset_id.split('.')

        # secure the target and native variables via e3sm_to_cmip --info

        cmip_out = os.path.join(self._slurm_out, "CMIP6")
        freq = parent_dsid.split('.')[7]
        realm = "atm"

        var_info = get_e2c_info(cmip_var, freq, realm, data_path, cmip_out, metadata_file, tables_path)

        # build up parameters list for job config .yaml write
        parameters = dict()

        parameters['tables_path'] = tables_path
        parameters['data_path'] = data_path
        parameters['metadata_path'] = metadata_file

        parameters['std_var_list']   = var_info['natv_vars']      # for mlev, else no harm if empty
        parameters['std_cmor_list']  = var_info['cmip_vars']      # for mlev, else no harm if empty
        parameters['plev_var_list']  = var_info['natv_plev_vars'] # for plev, else no harm if empty
        parameters['plev_cmor_list'] = var_info['cmip_plev_vars'] # for plev, else no harm if empty

        parameters['vrt_map_path'] = self.config['vrt_map_path']  # not needed for mlev-only but no harm

        parameters.update(cwl_config)   # obtain frequency, num_workers, account, partition, e2c_timeout, slurm_timeout
        parameters.update(deriv_conf)   # obtain hrz_atm_map_path, mapfile, region_file, file_pattern, case_finder

        # write out the parameter file and setup the temp directory
        parameters_name = f"{experiment}-{cmip_model_version}-{variant}-{realm}-cmip-{freq}-{cmip_var}.yaml"
        parameters_path = os.path.join(self._slurm_out, parameters_name)
        with open(parameters_path, 'w') as outstream:
            yaml.dump(parameters, outstream)

        mlev = var_info['mlev']
        plev = var_info['plev']

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

        if not self.serial:
            parallel = "--parallel"
        else:
            parallel = ''

        self._cmd = f"cwltool --outdir {outpath} --tmpdir-prefix={self.tmpdir} {parallel} --preserve-environment UDUNITS2_XML_PATH {cwl_workflow_path} {parameters_path}"
