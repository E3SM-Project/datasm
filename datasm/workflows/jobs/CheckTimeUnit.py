from datasm.workflows.jobs import WorkflowJob

NAME = 'CheckTimeUnit'

class CheckTimeUnit(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = { '*-native-*': None }
        timename = 'time' if self.dataset.realm in ['atmos', 'land'] else 'Time'
        self._cmd = f'cd {self.scripts_path}; python check_time_units.py -q -p {self._job_workers} --time-name {timename} {self.dataset.latest_warehouse_dir}'

