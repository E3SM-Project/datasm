from warehouse.workflows.jobs import WorkflowJob

NAME = 'CheckTimeIndex'

class CheckTimeIndex(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME
        self.cmd = f''
