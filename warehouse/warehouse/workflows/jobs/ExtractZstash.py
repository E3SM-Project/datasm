from warehouse.workflows.jobs import WorkflowJob

NAME = 'ExtractZstash'

class ExtractZstash(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args,**kwargs)
        self.name = NAME
        self._cmd = ''
