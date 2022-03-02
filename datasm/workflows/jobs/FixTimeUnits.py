from datasm.workflows.jobs import WorkflowJob

NAME = 'FixTimeUnits'

class FixTimeUnits(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = { '*-native-*': None }
        self._cmd = f"""
cd {self.scripts_path}
python fix_time_units.py -q -p {self._job_workers} --time-units "{self.params["correct_units"]}" --time-offset {self.params["offset"]} {self.dataset.latest_warehouse_dir} {self.find_outpath()}
"""
