from warehouse.workflows.jobs import WorkflowJob

NAME = 'FixTimeUnits'

class FixTimeUnits(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._cmd = ''
        self._requires = {'atmos-native-mon': None, 'ocean-native-mon': None}
