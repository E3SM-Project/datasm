from warehouse.workflows.jobs import WorkflowJob

NAME = 'MoveToPublication'

class MoveToPublication(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._cmd = f"""
cd {self.scripts_path}
python move_to_publication.py --src-path {self.dataset.latest_warehouse_dir} --dst-path {self.dataset.latest_publication_dir}
"""

