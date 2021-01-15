from warehouse.jobs import WorkflowJob

NAME = 'CheckTimeUnit'

class CheckTimeUnit(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cmd = ''
