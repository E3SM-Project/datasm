from warehouse.workflows.jobs import WorkflowJob

NAME = 'RectifyTimeIndex'

class RectifyTimeIndex(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.name = NAME
        self.cmd = ''
