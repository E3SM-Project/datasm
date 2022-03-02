from warehouse.workflows.jobs import WorkflowJob
from pathlib import Path
from datetime import datetime
from pytz import UTC


NAME = 'MoveToPublication'


class MoveToPublication(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME

        # Need to CLEAN THIS UP!  Eliminate variant versioning.
        dst_version = self.dataset.pub_version
        if self.dataset.project == "E3SM":
            dst_version = UTC.localize(datetime.utcnow()).strftime("%Y%m%d")
        
        self._cmd = f"""
cd {self.scripts_path}
python move_to_publication.py --src-path {self.dataset.latest_warehouse_dir} --dst-path {Path(self.dataset.publication_path, 'v' + str(dst_version))}
"""
