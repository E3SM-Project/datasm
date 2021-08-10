from warehouse.workflows.jobs import WorkflowJob

NAME = 'GenerateLndTimeseries'

class GenerateLndTimeseries(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = { 'lnd-native-mon': None }
        self._cmd = ''
    
    def resolve_cmd(self):

        exclude = self._spec['projects']['E3SM'][self.dataset.model_version][self.dataset.experiment].get('except')
        variables = [x for x in self._spec['time-series']['atmos'] if x not in exclude]

        raw_dataset = self.requires['lnd-native-mon']

        flags = "-7 --dfl_lvl=1 --no_cll_msr "
        self._cmd = f"""
            ncclimo {flags} -v {','.join(variables)} -s {raw_dataset.start_year} -e {raw_dataset.end_year} --ypf=10 -i {raw_dataset.latest_warehouse_dir} --sgs_frac=landfrac
        """
    
    def render_cleanup(self):
        native_out = f"{self.dataset.latest_warehouse_dir}-tmp/"
        cmd = f"""
            if [ -d {native_out} ]; then
                rm -rf {native_out}
            fi        
        """
        return cmd
        
