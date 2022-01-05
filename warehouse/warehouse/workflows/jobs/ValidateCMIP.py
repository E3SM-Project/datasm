from warehouse.workflows.jobs import WorkflowJob

NAME = 'ValidateCMIP'


class ValidateCMIP(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = { '*-gr-*': None }
        print(f"DEBUG VALIDATE CMIP: We intend to issue: python validate_cmip.py {self.dataset.latest_warehouse_dir} {self.config['DEFAULT_PLOT_PATH']} {self.dataset.dataset_id}")

        self._cmd = f"""
            cd {self.scripts_path}
            python validate_cmip.py {self.dataset.latest_warehouse_dir} {self.config['DEFAULT_PLOT_PATH']} {self.dataset.dataset_id}
        """
