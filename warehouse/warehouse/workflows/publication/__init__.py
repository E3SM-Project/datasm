import os
import sys
from pathlib import Path
from time import sleep
from termcolor import colored, cprint
from warehouse.workflows import Workflow
from warehouse.dataset import Dataset, DatasetStatusMessage


NAME = 'Publication'
COMMAND = 'publish'

HELP_TEXT = """
Publish a set of E3SM datasets to ESGF. If used, the --data-path argument should be 
one level up from the data directory (which should be named vN where N is an integer 0 or greater), 
and will be used to hold the .status file and intermediate working directories for the workflow steps.
"""

class Publication(Workflow):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME.upper()
        self.pub_path = None

    def __call__(self, *args, **kwargs):
        from warehouse.warehouse import AutoWarehouse

        dataset_id = self.params['dataset_id']

        if (pub_base := self.params.get('publication_path')):
            self.pub_path = Path(pub_base)
            if not self.pub_path.exists():
                os.makedirs(self.pub_path.resolve())
        data_path = self.params.get('data_path')

        if data_path is not None:
            warehouse = AutoWarehouse(
                workflow=self,
                dataset_id=dataset_id,
                warehouse_path=data_path,
                publication_path=self.pub_path,
                serial=True,
                job_worker=self.job_workers,
                debug=self.debug)
        else:
            warehouse = AutoWarehouse(
                workflow=self,
                dataset_id=dataset_id,
                warehouse_path=self.params['warehouse_path'],
                publication_path=self.pub_path,
                serial=True,
                job_worker=self.job_workers,
                debug=self.debug)

        warehouse.setup_datasets(check_esgf=False)

        for dataset_id, dataset in warehouse.datasets.items():
            dataset.warehouse_base = Path(self.params['warehouse_path'])
            if data_path:
                dataset.warehouse_path = Path(data_path)

            if DatasetStatusMessage.PUBLICATION_READY.value not in dataset.status:
                dataset.status = DatasetStatusMessage.PUBLICATION_READY.value
        
        warehouse.start_listener()
        
        for dataset_id, dataset in warehouse.datasets.items():
            warehouse.start_datasets({dataset_id: dataset})

        while not warehouse.should_exit:
            sleep(2)
        
        for dataset_id, dataset in warehouse.datasets.items():
            color = "green" if "Pass" in dataset.status else "red"
            cprint(
                f"Publication complete, dataset {dataset_id} is in state {dataset.status}", color)
        
        sys.exit(0)

    @staticmethod
    def add_args(parser):
        parser = parser.add_parser(
            name=COMMAND,
            description=HELP_TEXT)
        parser = Workflow.add_args(parser)
        return COMMAND, parser

    @staticmethod
    def arg_checker(args):
        check_pass, _ = Workflow.arg_checker(args, COMMAND)
        if not check_pass:
            return False, COMMAND
        return True, COMMAND
