from e3sm_warehouse.workflows.jobs import WorkflowJob
import os

NAME = 'GenerateMapfile'

class GenerateMapfile(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        if self.dataset.project == 'CMIP6':
            pub_version = self.dataset.pub_version
        else:
            pub_version = int(self.dataset.pub_version) + 1
        self._cmd = f'''
cd {self.scripts_path}
python generate_mapfile.py -p {self._job_workers} --outpath {self.dataset.warehouse_path}{os.sep}{self.dataset.dataset_id}.map {self.dataset.latest_warehouse_dir} {self.dataset.dataset_id} {pub_version} --quiet
'''
