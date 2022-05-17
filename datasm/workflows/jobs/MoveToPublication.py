from pathlib import Path
from datasm.util import get_UTC_YMD
from datasm.workflows.jobs import WorkflowJob

NAME = 'MoveToPublication'


class MoveToPublication(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME

        dst_version = get_UTC_YMD()

        self._cmd = f"""
cd {self.scripts_path}
python move_to_publication.py --src-path {self.dataset.latest_warehouse_dir} --dst-path {Path(self.dataset.publication_path, 'v' + str(dst_version))}
"""
