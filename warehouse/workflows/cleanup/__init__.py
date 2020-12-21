from warehouse.workflows import Workflow
from warehouse.workflows.cleanup.jobs import EvictDataSet


class CleanUp(Workflow):

    def __init__(self):
        super().__init__()

    @staticmethod
    def add_args(parser):
        name = 'cleanup'
        p = parser.add_parser(
            name=name,
            help='Remove dataset directories from the warehouse')
        p.add_argument(
            '-p', '--path',
            required=True,
            help="path to the warehouse root")
        p.add_argument(
            '-d', '--dataset',
            required=True,
            help="the dataset_id to extract")
        return 'extraction', parser

    @staticmethod
    def arg_checker(args):
        name = 'extract'
        if not os.path.exists(args.path):
            print("The given path {args.path} does not exist")
            return name
        return True
