from e3sm_warehouse.workflows.jobs import WorkflowJob

NAME = 'GenerateIceCMIP'

class GenerateIceCMIP(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = { 'ice-native-mon': None }
        self._cmd = ''
