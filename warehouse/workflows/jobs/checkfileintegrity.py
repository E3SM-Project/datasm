from warehouse.workflows.jobs import WorkflowJob

NAME = 'CheckFileIntegrity'

class CheckFileIntegrity(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # import ipdb; ipdb.set_trace()
        self.name = NAME
        self._requires = { '*-*-*': None }
        self._cmd = f"""
cd {self.scripts_path}
python check_file_integrity.py -p {self._job_workers} {self.dataset.working_dir}
"""
