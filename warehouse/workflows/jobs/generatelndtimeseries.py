from warehouse.jobs import WorkflowJob

NAME = 'GenerateLndTimeseries'

class GenerateLndTimeseries(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cmd = ''
