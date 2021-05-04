import yaml
from warehouse.workflows.jobs import WorkflowJob


NAME = 'GenerateAtmMonTimeseries'

class GenerateAtmMonTimeseries(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = { 'atmos-native-mon': None }
        self._cmd = ""
    
    def resolve_cmd(self):
        with open(self._spec_path, 'r') as i:
            spec = yaml.load(i, Loader=yaml.SafeLoader)
        
        variables = spec['time-series']['atmos']
        self._cmd = f"ncclimo --ypf=10 -v {','.join(variables)} -j {self._job_workers} -s {self.dataset.start_year} -e {self.dataset.end_year} -i {self._requires['atmos-native-mon'].latest_warehouse_dir} -o {self.dataset.latest_warehouse_dir}-tmp   -O {self.find_outpath()} --map={self.config['grids']['ne30_to_180x360']}"
