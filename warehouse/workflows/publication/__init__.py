import os
from warehouse.workflows import Workflow

NAME = 'Publication'
COMMAND = 'publish'

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
            help='validate a raw dataset')
        parser = Workflow.add_args(parser)
        return COMMAND, parser

    @staticmethod
    def arg_checker(args):
        if not os.path.exists(args.path):
            print("The given path {args.path} does not exist")
            return False, COMMAND
        return True, COMMAND
