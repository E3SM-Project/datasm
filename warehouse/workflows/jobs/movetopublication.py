from warehouse.jobs import WorkflowJob

NAME = 'MoveToPublication'

class MoveToPublication(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cmd = ''
