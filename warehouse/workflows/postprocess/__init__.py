from warehouse.workflows import Workflow


class PostProcess(Workflow):

    def __init__(self):
        super().__init__()

    def __call__(self):
        ...

    @staticmethod
    def add_args(parser):
        name = 'postprocess'
        p = parser.add_parser(
            name=name,
            help='postprocess a dataset')
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
        name = 'postprocess'
        if not os.path.exists(args.path):
            print("The given path {args.path} does not exist")
            return name
        return True
