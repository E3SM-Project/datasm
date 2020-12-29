from warehouse.workflows import WorkflowJob


class EvictDataSet(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cmd = ''
