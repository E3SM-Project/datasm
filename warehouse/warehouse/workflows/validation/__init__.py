import sys
from pathlib import Path
from time import sleep
from warehouse.workflows import Workflow
from warehouse.dataset import Dataset, DatasetStatusMessage
from termcolor import colored, cprint


NAME = 'Validation'
COMMAND = 'validate'

HELP_TEXT = """
Runs the Validation workflow on a single dataset or list of datasets. If running on
a single dataset, the --data-path should point one level up from the data directory 
which should me named v0, the input path will be used to hold the .status file and intermediate working directories for the workflow steps. 

Multiple datasets can be run at once as long as the datasets exist under the --warehouse-path directory in the expected
faceted structure. This structure should mirror the E3SM publication directory structure.

The --dataset-id flag should be in the facet format of the ESGF project. 
    For CMIP6: CMIP6.ScenarioMIP.CCCma.CanESM5.ssp126.r12i1p2f1.Amon.wap.gn
    For E3SM: E3SM.1_0.historical.1deg_atm_60-30km_ocean.atmos.180x360.climo.164yr.ens5
"""


class Validation(Workflow):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME.upper()

    def __call__(self, *args, **kwargs):
        from warehouse.warehouse import AutoWarehouse

        dataset_ids = self.params['dataset_id']
        warehouse_path = self.params['warehouse_path']
        publication_path = self.params['publication_path']
        archive_path = self.params['archive_path']

        if (data_path := self.params.get('data_path')):
            warehouse = AutoWarehouse(
                workflow=self,
                dataset_id=dataset_ids,
                warehouse_path=data_path,
                publication_path=publication_path,
                archive_path=archive_path,
                serial=True,
                job_worker=self.job_workers,
                debug=self.debug)
        else:
            warehouse = AutoWarehouse(
                workflow=self,
                dataset_id=dataset_ids,
                warehouse_path=warehouse_path,
                publication_path=publication_path,
                archive_path=archive_path,
                serial=True,
                job_worker=self.job_workers,
                debug=self.debug)

        warehouse.setup_datasets(check_esgf=False)

        for dataset_id, dataset in warehouse.datasets.items():
            if data_path:
                dataset.warehouse_path = Path(data_path)

            if DatasetStatusMessage.VALIDATION_READY.value not in dataset.status:
                dataset.status = DatasetStatusMessage.VALIDATION_READY.value

        warehouse.start_listener()

        for dataset_id, dataset in warehouse.datasets.items():
            warehouse.start_datasets({dataset_id: dataset})

        while not warehouse.should_exit:
            sleep(2)

        for dataset_id, dataset in warehouse.datasets.items():
            color = "green" if "Pass" in dataset.status else "red"
            cprint(
                f"Validation complete, dataset {dataset_id} is in state {dataset.status}", color)

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
        return Workflow.arg_checker(args, COMMAND)
