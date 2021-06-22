from warehouse.workflows.jobs import WorkflowJob
from pathlib import Path

NAME = 'GenerateMapfile'

class GenerateMapfile(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._cmd = f'''
cd {self.scripts_path}
python generate_mapfile.py -p {self._job_workers} --outpath {Path(self.dataset.warehouse_path, '.mapfile')} {self.dataset.latest_warehouse_dir} {self.dataset.dataset_id} {int(self.dataset.pub_version) + 1}
'''
