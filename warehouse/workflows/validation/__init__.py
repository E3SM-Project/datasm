from warehouse.workflows import Workflow


class Validation(Workflow):

    def __init__(self):
        super().__init__()
    
    def __call__(self):
        ...

    @staticmethod
    def add_args(parser):
        name = 'validation'
        p = parser.add_parser(
            name=name,
            help='validate a raw dataset')
        p.add_argument(
            '-p', '--path',
            required=True,
            help="root path to the warehouse root directory")
        p.add_argument(
            '-d', '--dataset',
            required=True,
            help="the dataset_id to extract")
        return name, parser

    @staticmethod
    def arg_checker(args):
        name = 'validation'
        if not os.path.exists(args.path):
            print("The given path {args.path} does not exist")
            return name
        return True
