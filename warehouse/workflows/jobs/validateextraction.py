from warehouse.jobs import WorkflowJob

NAME = 'ValidateExtraction'


class ValidateExtraction(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cmd = ''
