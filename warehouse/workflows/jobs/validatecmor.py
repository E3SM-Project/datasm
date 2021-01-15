from warehouse.jobs import WorkflowJob

NAME = 'ValidateCmor'


class ValidateCmor(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cmd = ''
