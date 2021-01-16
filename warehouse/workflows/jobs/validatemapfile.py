from warehouse.workflows.jobs import WorkflowJob

NAME = 'ValidateMapfile'

class ValidateMapfile(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.name = NAME
        self.cmd = ''
