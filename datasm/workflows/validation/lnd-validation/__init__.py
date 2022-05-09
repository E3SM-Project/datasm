from datasm.workflows import Workflow

NAME = 'LNDValidation'

class LNDValidation(Workflow):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.name = NAME.upper()

    def __call__(self):
        ...
