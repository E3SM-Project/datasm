from pathlib import Path
from datasm.workflows.jobs import WorkflowJob

NAME = 'FixMapfilePaths'


class FixMapfilePaths(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        mapfile_path = Path(self.dataset.publication_path, f'{self.dataset.dataset_id}.map')
        if not mapfile_path.exists():
            raise ValueError(
                f"Dataset {self.dataset.dataset_id} does not have a mapfile at {mapfile_path}")

        self._cmd = f"""
cd {self.scripts_path}
python fix_mapfile_paths.py {self.params['mapfile_path']} {self.dataset.warehouse_base} {self.dataset.pub_base} {self.params['ware_name']} {self.params['pub_name']}
"""
