from pathlib import Path
from warehouse.workflows.jobs import WorkflowJob

NAME = 'FixMapfilePaths'


class FixMapfilePaths(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = {'*-*-*': None}
        mapfile_path = Path(self.dataset.warehouse_path, '.mapfile')
        if not mapfile_path.exists():
            raise ValueError(
                f"Dataset {self.dataset.dataset_id} does not have a mapfile at {mapfile_path}")
        self._cmd = f"""
cd {self.scripts_path}
chmod +x fix_mapfile_paths.sh
bash fix_mapfile_paths.sh {mapfile_path} {self.dataset.warehouse_base} {self.dataset.pub_base}
"""
