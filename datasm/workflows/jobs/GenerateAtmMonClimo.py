import re
import os
from pathlib import Path
from datasm.util import log_message
from datasm.workflows.jobs import WorkflowJob

NAME = 'GenerateAtmMonClimo'

class GenerateAtmMonClimo(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = { 'atmos-native-mon': None }
        self._cmd = ''

    def resolve_cmd(self):

        raw_dataset = self.requires['atmos-native-mon']

        start = self.dataset.start_year
        end = self.dataset.end_year

        inpath = raw_dataset.latest_warehouse_dir
        outpath =  self.dataset.latest_warehouse_dir

        dsid = f"{self.dataset.dataset_id}"
        model = dsid.split(".")[1][0]
        native_resolution = dsid.split(".")[3]

        filename = Path(inpath).glob('*.nc').__next__()
        if model == "1":
            idx = re.search('\.cam\.h\d\.', filename.name)
            if native_resolution == "1deg_atm_60-30km_ocean":
                mapkey = "v1_ne30_to_180x360"
            else: # assume
                mapkey = "v1_ne120np4_to_cmip6_720x1440"
        else: # assume v2
            idx = re.search('\.eam\.h\d\.', filename.name)
            if native_resolution in [ "1deg_atm_60-30km_ocean", "LR" ]:
                mapkey = "v2_ne30_to_180x360"
            else: # assume
                mapkey = "v2_ne120pg2_to_cmip6_720x1440"
        casename = filename.name[:idx.start()]
        # NOTE: available grids are defined in resources/datasm_config.yaml
        map_path = self.config['grids'][mapkey]

        native_out = f"{os.environ.get('TMPDIR', '/tmp')}{os.sep}{self.dataset.dataset_id}/climo/"
        self._cmd = f"""
            cd {self.scripts_path}
            ncclimo -c {casename} -s {start} -e {end} -i {inpath} -r {map_path} -o {native_out} -O {outpath}
        """


    def render_cleanup(self):
        native_out = f"{os.environ.get('TMPDIR', '/tmp')}{os.sep}{self.dataset.dataset_id}/climo/"
        cmd = f"""
            if [ -d {native_out} ]; then
                rm -rf {native_out}
            fi
        """
        return cmd
