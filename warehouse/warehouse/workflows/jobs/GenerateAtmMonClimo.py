import re
import os
from pathlib import Path
from warehouse.util import log_message
from warehouse.workflows.jobs import WorkflowJob

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
        native_resolution = dsid.split(".")[3]
        # NOTE: available grids are defined in resources/warehouse_config.yaml
        mapkey = "ne30_to_180x360"
        if native_resolution == "0_25deg_atm_18-6km_ocean":
            mapkey = "ne120np4_to_cmip6_720x1440"
            # or else maybe "ne120np4_to_cmip6_180x360"

        map_path = self.config['grids'][mapkey]

        filename = Path(inpath).glob('*.nc').__next__()
        idx = re.search('\.cam\.h\d\.', filename.name)
        casename = filename.name[:idx.start()]

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
