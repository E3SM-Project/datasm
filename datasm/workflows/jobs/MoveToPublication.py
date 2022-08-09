from pathlib import Path
from datasm.util import get_UTC_YMD, get_dataset_version_from_file_metadata, log_message
from datasm.workflows.jobs import WorkflowJob

NAME = 'MoveToPublication'


class MoveToPublication(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME

        log_message("info", f"applying publication path: {self.dataset.publication_path}")
        dst_version = get_dataset_version_from_file_metadata(self.dataset.latest_warehouse_dir)
        if dst_version == 'NONE':
            log_message("info", "Obtaining dataset version from current date - not from metadata")
            dst_version = 'v' + get_UTC_YMD()
        else:
            log_message("info", f"Obtaining dataset version from metadata ({dst_version})")

        self._cmd = f"""
cd {self.scripts_path}
python move_to_publication.py --src-path {self.dataset.latest_warehouse_dir} --dst-path {Path(self.dataset.publication_path, str(dst_version))}
"""
