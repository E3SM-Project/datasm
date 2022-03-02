from e3sm_warehouse.workflows.jobs import WorkflowJob

NAME = 'CheckTime'

class CheckTime(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = { '*-native-*': None }
        self._cmd = f"""
cd {self.scripts_path}
python check_time_values.py -q -j {self._job_workers} {self.dataset.latest_warehouse_dir}
"""
