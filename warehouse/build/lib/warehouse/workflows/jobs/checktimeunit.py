from warehouse.workflows.jobs import WorkflowJob

NAME = 'CheckTimeUnit'

class CheckTimeUnit(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = { 'atmos-native-*': None }
        timename = 'time' if self.dataset.realm == 'atmos' else 'Time'
        self._cmd = f'cd {self.scripts_path}; python check_time_units.py -p {self._job_workers} --time-name {timename} {self.dataset.working_dir}'
