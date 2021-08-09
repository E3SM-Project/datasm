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
        
        exclude = self._spec['projects']['E3SM'][self.dataset.model_version][self.dataset.experiment].get('except')
        variables = [x for x in self._spec['time-series']['atmos'] if x not in exclude]
        self._cmd = f"ncclimo --ypf=10 -v {','.join(variables)} -j {self._job_workers} -s {self.dataset.start_year} -e {self.dataset.end_year} -i {self._requires['atmos-native-mon'].latest_warehouse_dir} -o {self.dataset.latest_warehouse_dir}-tmp   -O {self.find_outpath()} --map={self.config['grids']['ne30_to_180x360']}"
