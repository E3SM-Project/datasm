from datasm.workflows.jobs import WorkflowJob

NAME = 'EvictDataset'

class EvictDataset(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args,**kwargs)
        self.name = NAME
        self._cmd = ''
