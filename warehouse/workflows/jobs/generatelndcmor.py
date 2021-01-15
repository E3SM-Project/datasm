from warehouse.jobs import WorkflowJob

NAME = 'GenerateLndCmor'

class GenerateLndCmor(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cmd = ''
