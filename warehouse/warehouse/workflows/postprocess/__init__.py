import os
import sys
from pathlib import Path
from time import sleep
from termcolor import colored, cprint
from warehouse.workflows import Workflow
from warehouse.util import log_message
from warehouse.dataset import DatasetStatusMessage

NAME = 'PostProcess'
COMMAND = 'postprocess'

HELP_TEXT = """
Run post-processing jobs to generate climatologies, regridded time-series, and CMIP6 datasets
"""


class PostProcess(Workflow):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.name = NAME.upper()
        parallel = self.params.get('parallel')
        self.serial = False if parallel else True
        self.metadata_path = None
        log_message(
            'info', f'initializing job {self.name} for {self.dataset.dataset_id}')

    def __call__(self):
        from warehouse.warehouse import AutoWarehouse

        dataset_id = self.params['dataset_id']
        log_message("info", f'Starting with datasets {dataset_id}')

        if (metadata_path := self.params.get('metadata_path')):
            self.metadata_path = Path(metadata_path)
        data_path = self.params.get('data_path')
        tmpdir = self.params.get('tmp')

        if data_path is not None:
            warehouse = AutoWarehouse(
                workflow=self,
                dataset_id=dataset_id,
                warehouse_path=data_path,
                serial=self.serial,
                job_worker=self.job_workers,
                debug=self.debug,
                tmpdir=tmpdir)
        else:
            warehouse = AutoWarehouse(
                workflow=self,
                dataset_id=dataset_id,
                warehouse_path=self.params['warehouse_path'],
                serial=self.serial,
                job_worker=self.job_workers,
                debug=self.debug,
                tmpdir=tmpdir)

        warehouse.setup_datasets(check_esgf=False)

        for dataset_id, dataset in warehouse.datasets.items():
            dataset.warehouse_base = Path(self.params['warehouse_path'])
            if data_path:
                dataset.warehouse_path = Path(data_path)

            if DatasetStatusMessage.POSTPROCESS_READY.value not in dataset.status:
                dataset.status = DatasetStatusMessage.POSTPROCESS_READY.value

        warehouse.start_listener()

        for dataset_id, dataset in warehouse.datasets.items():
            warehouse.start_datasets({dataset_id: dataset})

        while not warehouse.should_exit:
            sleep(2)

        for dataset_id, dataset in warehouse.datasets.items():
            color = "green" if "Pass" in dataset.status else "red"
            cprint(
                f"Postprocessing complete, dataset {dataset_id} is in state {dataset.status}", color)
        sys.exit(0)

    @staticmethod
    def add_args(parser):
        parser = parser.add_parser(
            name=COMMAND,
            description='postprocess datasets')
        parser.add_argument(
            '--parallel',
            action="store_true",
            help="Submit CWL workflows with the --parallel flag, to run all their steps in parallel (where possible)")
        parser.add_argument(
            '--tmp',
            required=False,
            default=f"{os.environ.get('TMPDIR')}",
            help=f"the directory to use for temp output, default is the $TMPDIR environment variable which you have set to: {os.environ.get('TMPDIR')}")
        parser = Workflow.add_args(parser)
        return COMMAND, parser

    @staticmethod
    def arg_checker(args):
        return Workflow.arg_checker(args, COMMAND)
