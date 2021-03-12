from warehouse.workflows.jobs import WorkflowJob

NAME = 'FixMapfilePaths'

class FixMapfilePaths(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._cmd = f"""
cd {self.scripts_path}
./fix_mapfile_paths.sh {self.params["mapfile_path"]}
"""

