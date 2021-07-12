from warehouse.workflows import Workflow
# from warehouse.jobs import EvictDataset
from warehouse.util import log_message

COMMAND = 'cleanup'
NAME = 'CleanUp'

class CleanUp(Workflow):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.name = NAME.upper()
        log_message('info', f'initializing workflow {self.name}')

    def __call__(self):
        log_message('info', f'starting workflow {self.name}')

    @staticmethod
    def add_args(parser):
        parser = parser.add_parser(
            name=COMMAND,
            description='Remove dataset directories from the warehouse')
        parser = Workflow.add_args(parser)
        return COMMAND, parser

    @staticmethod
    def arg_checker(args):
        if not os.path.exists(args.path):
            log_message('error',f"The given path {args.path} does not exist")
            return False, COMMAND
        return True, COMMAND
