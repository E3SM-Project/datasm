import re
import os
from pathlib import Path
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

        start = raw_dataset.start_year
        end = raw_dataset.end_year
        inpath = raw_dataset.latest_warehouse_dir
        outpath =  self.dataset.latest_warehouse_dir
        map_path = self.config['grids']['ne30_to_180x360']

        filename = Path(inpath).glob('*.nc').__next__()
        idx = re.search('\.cam\.h\d\.', filename.name)
        casename = filename[:idx.start()]

        native_out = f"{os.environ.get('TMPDIR', '/tmp')}{os.sep}{self.dataset.dataset_id}/climo/"
        self._cmd = f"""
            cd {self.scripts_path}
            ncclimo -c {casename} -a sdd -s {start} -e {end} -i {inpath} -r {map_path} -o {native_out} -O {outpath} --no_amwg_links
        """
    
    
    def render_cleanup(self):
        native_out = f"{os.environ.get('TMPDIR', '/tmp')}{os.sep}{self.dataset.dataset_id}/climo/"
        cmd = f"""
            if [ -d {native_out} ]; then
                rm -rf {native_out}
            fi        
        """
        return cmd
