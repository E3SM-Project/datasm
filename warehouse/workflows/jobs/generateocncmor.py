from warehouse.workflows.jobs import WorkflowJob

NAME = 'GenerateOcnCmor'

class GenerateOcnCmor(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.name = NAME
        self.cmd = ''
