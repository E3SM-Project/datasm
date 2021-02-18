from warehouse.workflows.jobs import WorkflowJob

NAME = 'GenerateAtmCmor'

class GenerateAtmCmor(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.name = NAME
        self.cmd = ''
