from warehouse.workflows.jobs import WorkflowJob

NAME = 'GenerateAtmTimeseries'

class GenerateAtmTimeseries(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.name = NAME
        self.cmd = ''
