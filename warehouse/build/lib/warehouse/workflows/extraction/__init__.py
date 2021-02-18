import os
from warehouse.workflows import Workflow
# from warehouse.workflows.extraction.jobs import (
#     ExtractionValidate,
#     ZstashExtract
# )

COMMAND = 'extract'
NAME = 'Extraction'

class Extraction(Workflow):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.name = NAME.upper()

    def __call__(self):
        ...

    @staticmethod
    def add_args(parser):
        parser = parser.add_parser(
            name=COMMAND,
            description='extract datasets from zstash and perform validation')
        parser.add_argument(
            '-z', '--zstash',
            required=True,
            help="Path to zstash directory")
        parser = Workflow.add_args(parser)
        return COMMAND, parser

    @staticmethod
    def arg_checker(args):
        if not os.path.exists(args.path):
            print("The given path {args.path} does not exist")
            return False, COMMAND
        if not not os.path.exists(args.zstash):
            print("The given path {args.zstash} does not exist")
            return False, COMMAND
        return True, COMMAND
