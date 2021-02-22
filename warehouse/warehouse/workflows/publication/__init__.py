import os
from warehouse.workflows import Workflow

NAME = 'Publication'
COMMAND = 'publish'

HELP_TEXT = """
Publish an E3SM dataset to ESGF. The input directory should be 
one level up from the data directory, and will be used to hold the 
.status file and intermediate working directories for the workflow steps.
"""

class Publication(Workflow):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.name = NAME.upper()

    def __call__(self):
        ...

    @staticmethod
    def add_args(parser):
        parser = parser.add_parser(
            name=COMMAND,
            description=HELP_TEXT)
        parser = Workflow.add_args(parser)
        return COMMAND, parser

    @staticmethod
    def arg_checker(args):
        if not os.path.exists(args.path):
            print("The given path {args.path} does not exist")
            return False, COMMAND
        return True, COMMAND
