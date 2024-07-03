import os
from pathlib import Path

import yaml
from datasm.workflows.jobs import WorkflowJob
from datasm.util import log_message, get_e2c_info, parent_native_dsid, latest_data_vdir, prepare_cmip_job_metadata, derivative_conf

NAME = 'GenerateCMIP'


class GenerateCMIP(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = {'*-native-*': None}
        self._cmd = ''

    def resolve_cmd(self):

        target_dsid = self.dataset.dataset_id
        target_file = f"to_generate_{target_dsid}"

        with open(target_file, 'w') as file:
            file.write(f"{target_dsid}")
        execmd = os.path.join(self.scripts_path,"dsm_generate_CMIP6.sh")

        self._cmd = f"{execmd} WORK {target_file}"
