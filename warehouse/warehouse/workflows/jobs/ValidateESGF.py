from warehouse.workflows.jobs import WorkflowJob

NAME = 'ValidateEsgf'

class ValidateEsgf(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._cmd = f"""
            cd {self.scripts_path}
            python validate_esgf.py --dataset-id {self.dataset.dataset_id}
        """
