from warehouse.workflows.jobs import WorkflowJob

NAME = 'ValidateExtraction'


class ValidateExtraction(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.name = NAME
        self.cmd = ''
