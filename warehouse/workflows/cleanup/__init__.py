from warehouse.workflows import Workflow
from warehouse.workflows.cleanup.jobs import EvictDataSet

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
        p = parser.add_parser(
            name=COMMAND,
            help='Remove dataset directories from the warehouse')
        p.add_argument(
            '-p', '--path',
            required=True,
            help="path to the warehouse root")
        p.add_argument(
            '-d', '--dataset',
            required=True,
            help="the dataset_id to extract")
        return COMMAND, parser

    @staticmethod
    def arg_checker(args):
        if not os.path.exists(args.path):
            print("The given path {args.path} does not exist")
            return False, COMMAND
        return True, COMMAND
