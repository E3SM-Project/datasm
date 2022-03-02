from pathlib import Path
from warehouse.workflows.jobs import WorkflowJob

NAME = 'ValidateMapfile'

class ValidateMapfile(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._cmd = f"""
cd {self.scripts_path}
python validate_mapfile.py --data-path {self.dataset.latest_warehouse_dir} --mapfile {self.params['mapfile_path']}
"""
