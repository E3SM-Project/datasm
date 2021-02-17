from warehouse.workflows import Workflow
# from warehouse.jobs import EvictDataset

COMMAND = 'cleanup'
NAME = 'CleanUp'

class CleanUp(Workflow):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.name = NAME.upper()

    def __call__(self):
        ...

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
            print("The given path {args.path} does not exist")
            return False, COMMAND
        return True, COMMAND
