from warehouse.workflows.jobs import WorkflowJob

NAME = 'GenerateOceanCMIP'

class GenerateOceanCMIP(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self._requires = { 
            'ocean-native-mon': None,
            'atmos-native-mon': None
        }
        self._cmd = ''
