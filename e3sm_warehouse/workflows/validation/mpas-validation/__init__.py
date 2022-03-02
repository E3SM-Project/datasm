from e3sm_warehouse.workflows import Workflow

NAME = 'MPASValidation'

class MPASValidation(Workflow):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.name = NAME.upper()

    def __call__(self):
        ...
