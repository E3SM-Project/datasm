from warehouse.workflows.jobs import WorkflowJob

NAME = 'ValidateCMIP'


class ValidateCMIP(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = { '*-cmip-*': None }
        self._cmd = f"""
            cd {self.scripts_path}
            python validate_cmip.py {self.dataset.latest_pub_dir} {self.params['plot_path']} {self.dataset.dataset_id}
        """
