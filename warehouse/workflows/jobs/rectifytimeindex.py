from warehouse.jobs import WorkflowJob

NAME = 'RectifyTimeIndex'

class RectifyTimeIndex(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cmd = ''
