from datasm.workflows.jobs import WorkflowJob
import os

NAME = 'GenerateMapfile'

class GenerateMapfile(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._cmd = f'''
cd {self.scripts_path}
python generate_mapfile.py -p {self._job_workers} --outpath {self.dataset.publication_path}{os.sep}{self.dataset.dataset_id}.map {self.dataset.latest_pub_dir} {self.dataset.dataset_id} {self.dataset.pub_version} --quiet
'''
