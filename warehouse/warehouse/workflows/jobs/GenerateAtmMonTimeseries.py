from warehouse.workflows.jobs import WorkflowJob


NAME = 'GenerateAtmMonTimeseries'

class GenerateAtmMonTimeseries(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = { 'atmos-native-mon': None }
        self._cmd = ""
    
    def resolve_cmd(self):
        
        exclude = self._spec['project']['E3SM'][self.dataset.model_version][self.dataset.experiment].get('except', [])
        variables = [x for x in self._spec['time-series']['atmos'] if x not in exclude]

        raw_dataset = self.requires['atmos-native-mon']
        native_out = f"{self.dataset.latest_warehouse_dir}-tmp/"

        start = self.dataset.start_year
        end = self.dataset.end_year
        
        dsid = f"{self.dataset.dataset_id}"
        native_resolution = dsid.split(".")[3]
        # NOTE: available grids are defined in resources/warehouse_config.yaml
        mapkey = "ne30_to_180x360"
        if native_resolution == "0_25deg_atm_18-6km_ocean":
            mapkey = "ne120np4_to_cmip6_720x1440"
            # or else maybe "ne120np4_to_cmip6_180x360"

        map_path = self.config['grids'][mapkey]

        self._cmd = f"""
            ncclimo --ypf=10 -v {','.join(variables)} -j {self._job_workers} -s {start} -e {end} -i {raw_dataset.latest_warehouse_dir} -o {native_out}  -O {self.find_outpath()} --map={map_path}
        """
    
    def render_cleanup(self):
        native_out = f"{self.dataset.latest_warehouse_dir}-tmp/"
        cmd = f"""
            if [ -d {native_out} ]; then
                rm -rf {native_out}
            fi        
        """
        return cmd
