import os
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

        case = raw_dataset.experiment
        start = raw_dataset.start_year
        end = raw_dataset.end_year
        inpath = raw_dataset.latest_warehouse_dir
        outpath =  self.dataset.latest_warehouse_dir
        map_path = self.config['grids']['ne30_to_180x360']
        self._cmd = f"""
            cd {self.scripts_path}
            ncclimo -c {case} -a sdd -s {start} -e {end} -i {inpath} -r {map_path} -o {os.environ['TMPDIR']}{self.dataset.dataset_id}/climo/ -O {outpath} --no_amwg_links
        """
