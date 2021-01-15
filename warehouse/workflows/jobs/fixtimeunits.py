from warehouse.jobs import WorkflowJob

NAME = 'FixTimeUnits'

class FixTimeUnits(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cmd = ''
