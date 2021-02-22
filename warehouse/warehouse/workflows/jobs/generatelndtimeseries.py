from warehouse.workflows.jobs import WorkflowJob

NAME = 'GenerateLndTimeseries'

class GenerateLndTimeseries(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.name = NAME
        self.cmd = ''
