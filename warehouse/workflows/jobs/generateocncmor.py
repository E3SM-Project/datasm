from warehouse.jobs import WorkflowJob

NAME = 'GenerateOcnCmor'

class GenerateOcnCmor(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cmd = ''
