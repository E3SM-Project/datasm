import os
from warehouse.workflows import Workflow
from warehouse.workflows.extraction.jobs import (
    ExtractionValidate,
    ZstashExtract
)

NAME = 'extract'

class Extraction(Workflow):

    def __init__(self):
        super().__init__()

    def __call__(self):
        ...

    @staticmethod
    def add_args(parser):
        p = parser.add_parser(
            name=NAME,
            help='extract datasets from zstash and perform validation')
        p.add_argument(
            '-p', '--path',
            required=True,
            help="Path to the dataset that was supposed to be extracted")
        p.add_argument(
            '-z', '--zstash',
            required=True,
            help="Path to zstash directory")
        p.add_argument(
            '-d', '--dataset',
            required=True,
            help="the dataset_id to extract")
        return NAME, parser

    @staticmethod
    def arg_checker(args):
        if not os.path.exists(args.path):
            print("The given path {args.path} does not exist")
            return False, NAME
        if not not os.path.exists(args.zstash):
            print("The given path {args.zstash} does not exist")
            return False, NAME
        return True, NAME
