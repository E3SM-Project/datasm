from pathlib import Path
from datasm.workflows.jobs import WorkflowJob
from datasm.util import log_message, derivative_conf

NAME = 'GenerateLndTimeseries'

class GenerateLndTimeseries(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = { 'land-native-mon': None }
        self._cmd = ''

    def resolve_cmd(self):

        exclude = self._spec['project']['E3SM'][self.dataset.model_version][self.dataset.experiment].get('except', [])
        variables = [x for x in self._spec['time-series']['land'] if x not in exclude]

        dsid = f"{self.dataset.dataset_id}"
        native_resolution = dsid.split(".")[3]

        # NOTE: available grids are defined in resources/datasm_config.yaml
        parameters = derivative_conf(self.dataset.dataset_id, self.config['e3sm_resource_path'])
        map_path = parameters['hrz_atm_map_path']

        raw_dataset = self.requires['land-native-mon']
        native_out = f"{self.dataset.latest_warehouse_dir}-tmp/"

        start = self.dataset.start_year
        end = self.dataset.end_year

        # this should be calculated dynamically, rounded to something standard
        # see: /p/user_pub/e3sm/bartoletti1/Projects/Dynamic_YPF_Calculation
        ypf=50

        flags = "-7 -P elm --dfl_lvl=1 --no_cell_measures "
        self._cmd = f"""
            ncclimo {flags} -v {','.join(variables)} -s {start} -e {end} -o {native_out} --map={map_path}  -O {self.dataset.latest_warehouse_dir} --ypf={ypf} -i {raw_dataset.latest_warehouse_dir} --sgs_frc={Path(raw_dataset.latest_warehouse_dir).glob('*.nc').__next__()}/landfrac
        """
        log_message("info", f"resolve_cmd: created self._cmd = {self._cmd}")

    def render_cleanup(self):
        native_out = f"{self.dataset.latest_warehouse_dir}-tmp/"
        cmd = f"""
            if [ -d {native_out} ]; then
                rm -rf {native_out}
            fi
        """
        return cmd

