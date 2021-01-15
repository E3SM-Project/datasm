from warehouse.jobs import WorkflowJob

NAME = 'GenerateAtmCmor'

class GenerateAtmCmor(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cmd = ''
