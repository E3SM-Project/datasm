from warehouse.workflows import Workflow

NAME = 'Publication'
COMMAND = 'publish'

class Publication(Workflow):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)

    def __call__(self):
        ...

    @staticmethod
    def add_args(parser):
        p = parser.add_parser(
            name=COMMAND,
            help='publish a dataset to ESGF')
        p.add_argument(
            '-p', '--path',
            required=True,
            help="root path to the warehouse root directory")
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
