from pathlib import Path
from datasm.util import get_UTC_YMD, get_dataset_version_from_file_metadata
from datasm.workflows.jobs import WorkflowJob

NAME = 'MoveToPublication'


class MoveToPublication(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME

        dst_version = get_dataset_version_from_file_metadata(self.dataset.latest_warehouse_dir)
        if dst_version == 'NONE':
            dst_version = 'v' + get_UTC_YMD()

        self._cmd = f"""
cd {self.scripts_path}
python move_to_publication.py --src-path {self.dataset.latest_warehouse_dir} --dst-path {Path(self.dataset.publication_path, str(dst_version))}
"""
