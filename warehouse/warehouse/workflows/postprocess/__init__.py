import os
import sys
from pathlib import Path
from time import sleep
from termcolor import colored, cprint
from warehouse.workflows import Workflow
from warehouse.dataset import Dataset, DatasetStatusMessage

NAME = 'Post-Process'
COMMAND = 'postprocess'

HELP_TEXT = """
Run post-processing jobs to generate climatologies, regridded time-series, and CMIP6 datasets
"""

class PostProcess(Workflow):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.name = NAME.upper()
        self.metadata_path = None

    def __call__(self):
        from warehouse.warehouse import AutoWarehouse

        dataset_id = self.params['dataset_id']

        if (metadata_path := self.params.get('metadata_path')):
            self.metadata_path = Path(metadata_path)
        data_path = self.params.get('data_path')

        if data_path is not None:
            warehouse = AutoWarehouse(
                workflow=self,
                dataset_id=dataset_id,
                warehouse_path=data_path,
                serial=True,
                job_worker=self.job_workers,
                debug=self.debug)
        else:
            warehouse = AutoWarehouse(
                workflow=self,
                dataset_id=dataset_id,
                warehouse_path=self.params['warehouse_path'],
                serial=True,
                job_worker=self.job_workers,
                debug=self.debug)

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
            description='postprocess a dataset')
        parser = Workflow.add_args(parser)
        return COMMAND, parser

    @staticmethod
    def arg_checker(args):
        if not os.path.exists(args.path):
            print("The given path {args.path} does not exist")
            return False, COMMAND
        return True, COMMAND
