from warehouse.workflows.jobs import WorkflowJob

NAME = 'PublishEsgf'


class PublishEsgf(WorkflowJob):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.name = NAME
        self.cmd = ''
