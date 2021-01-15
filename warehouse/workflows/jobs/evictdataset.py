from warehouse.jobs import WorkflowJob

NAME = 'EvictDataset'

class EvictDataset(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cmd = ''
