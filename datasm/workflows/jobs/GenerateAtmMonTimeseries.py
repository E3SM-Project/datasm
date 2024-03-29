from datasm.workflows.jobs import WorkflowJob
from datasm.util import log_message, derivative_conf


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

        # NOTE: available grids are defined in resources/datasm_config.yaml
        parameters = derivative_conf(self.dataset.dataset_id, self.config['e3sm_resource_path'])
        
        data_path = raw_dataset.latest_warehouse_dir
        log_message("error",f"FAKE_ERROR:{__name__}:resolve_cmd: data_path = {data_path}")

        map_path = parameters['hrz_atm_map_path']

        # this should NOT be the Pub Dir!
        # how about self.config['DEFAULT_WAREHOUSE_PATH']
        out_path = self.find_outpath()

        self._cmd = f"""
            ncclimo --ypf=50 -v {','.join(variables)} -j {self._job_workers} -s {start} -e {end} -i {data_path} -o {native_out}  -O {out_path} --map={map_path}
        """
        log_message("info", f"resolve_cmd: cmd = {self._cmd}")

    def render_cleanup(self):
        native_out = f"{self.dataset.latest_warehouse_dir}-tmp/"
        cmd = f"""
            if [ -d {native_out} ]; then
                rm -rf {native_out}
            fi
        """
        return cmd
