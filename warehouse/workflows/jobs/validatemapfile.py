from warehouse.jobs import WorkflowJob

NAME = 'ValidateMapfile'

class ValidateMapfile(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cmd = ''
