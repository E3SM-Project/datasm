from warehouse.jobs import WorkflowJob

NAME = 'FixMapfilePaths'

class FixMapfilePaths(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cmd = ''
