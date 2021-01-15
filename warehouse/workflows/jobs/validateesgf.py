from warehouse.jobs import WorkflowJob

NAME = 'ValidateEsgf'

class ValidateEsgf(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cmd = ''
