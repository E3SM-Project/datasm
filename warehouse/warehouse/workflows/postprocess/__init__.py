import os
from warehouse.workflows import Workflow
from warehouse.util import log_message

NAME = 'PostProcess'
COMMAND = 'postprocess'

class PostProcess(Workflow):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.name = NAME.upper()
        log_message('info',f'initializing job {self.name} for {self.dataset.dataset_id}')

    def __call__(self):
        log_message('info',f'starting job {self.name} for {self.dataset.dataset_id}')

    @staticmethod
    def add_args(parser):
        parser = parser.add_parser(
            name=COMMAND,
            description='postprocess a dataset')
        parser = Workflow.add_args(parser)
        return COMMAND, parser

    @staticmethod
    def arg_checker(args):
        if not os.path.exists(args.path):
            log_message('error',f"The given path {args.path} does not exist")
            return False, COMMAND
        return True, COMMAND
