import os
from e3sm_warehouse.workflows import Workflow
# from e3sm_warehouse.workflows.extraction.jobs import (
#     ExtractionValidate,
#     ZstashExtract
# )
from e3sm_warehouse.util import setup_logging, log_message

COMMAND = 'extract'
NAME = 'Extraction'

class Extraction(Workflow):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.name = NAME.upper()
        log_message('info', f'initializing workflow {self.name}')

    def __call__(self):
        log_message('info',f'starting workflow {self.name}')

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
            log_message('error',f"The given path {args.path} does not exist")
            return False, COMMAND
        if not not os.path.exists(args.zstash):
            log_message('error',f"The given path {args.zstash} does not exist")
            return False, COMMAND
        return True, COMMAND
