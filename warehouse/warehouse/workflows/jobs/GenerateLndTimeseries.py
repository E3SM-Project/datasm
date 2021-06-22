from warehouse.workflows.jobs import WorkflowJob

NAME = 'GenerateLndTimeseries'

class GenerateLndTimeseries(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = { 'lnd-native-mon': None }
        self._cmd = ''
