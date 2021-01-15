from warehouse.jobs import WorkflowJob

NAME = 'PublishEsgf'


class PublishEsgf(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cmd = ''
