import os
import sys
from pathlib import Path
from time import sleep
from warehouse.workflows import Workflow
from warehouse.dataset import Dataset, DatasetStatusMessage

NAME = 'Publication'
COMMAND = 'publish'

HELP_TEXT = """
Publish an E3SM dataset to ESGF. The input directory should be 
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

        dataset_id = self.params['dataset_id'].pop()

        if (pub_base := self.params.get('publication_base')):
            self.pub_path = Path(pub_base, dataset_id.replace('.', os.sep))
        else:
            self.pub_path = Path(self.params['publication_path'])
        if not self.pub_path.exists():
            os.makedirs(self.pub_path.resolve())
        
        if (data_path := self.params.get('data_path')):
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
                publication_path=self.pub_path,
                serial=True,
                job_worker=self.job_workers,
                debug=self.debug)

        warehouse.setup_datasets(check_esgf=False)
        dataset_id, dataset = next(iter(warehouse.datasets.items()))
        dataset.warehouse_path = Path(data_path)

        warehouse.start_listener()

        if DatasetStatusMessage.PUBLICATION_READY.value not in dataset.status:
            dataset.status = DatasetStatusMessage.PUBLICATION_READY.value
        else:
            warehouse.start_datasets()

        while not warehouse.should_exit:
            sleep(2)
        print(
            f"Publication complete, dataset {dataset.dataset_id} is in state {dataset.status}")
        sys.exit(0)

    @staticmethod
    def add_args(parser):
        parser = parser.add_parser(
            name=COMMAND,
            description=HELP_TEXT)
        parser.add_argument(
            '--publication-base',
            type=str,
            help="Base path for the publication directory structure. If it doesnt "
                 "already exist, the facet structure for the dataset will be created.")
        parser = Workflow.add_args(parser)
        return COMMAND, parser

    @staticmethod
    def arg_checker(args):
        check_pass, _ = Workflow.arg_checker(args, COMMAND)
        if not check_pass:
            return False, COMMAND
        if not args.publication_base:
            print("Please supply either the base path where the "
                  "publication directory structure should be generated.")
            return False, COMMAND
        return True, COMMAND
