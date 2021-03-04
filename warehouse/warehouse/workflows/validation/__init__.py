import sys
from pathlib import Path
from time import sleep
from warehouse.workflows import Workflow
from warehouse.dataset import Dataset, DatasetStatusMessage


NAME = 'Validation'
COMMAND = 'validate'

HELP_TEXT = f"""
Runs the Validation workflow on a single dataset. The input directory should be 
one level up from the data directory which should me named v0, the input path 
will be used to hold the .status file and intermediate working directories for the workflow steps. 

The --dataset-id flag should be in the facet format of the ESGF project. 
    For CMIP6: CMIP6.ScenarioMIP.CCCma.CanESM5.ssp126.r12i1p2f1.Amon.wap.gn
    For E3SM: E3SM.1_0.historical.1deg_atm_60-30km_ocean.atmos.180x360.climo.164yr.ens5
"""


class Validation(Workflow):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = NAME.upper()
        self.datasets = None

    def __call__(self, *args, **kwargs):
        from warehouse.warehouse import AutoWarehouse

        dataset_ids = self.params['dataset_id']

        if (data_path := self.params.get('data_path')):
            warehouse = AutoWarehouse(
                workflow=self,
                dataset_id=dataset_ids,
                warehouse_path=data_path,
                serial=True,
                job_worker=self.job_workers,
                debug=self.debug)
        else:
            warehouse = AutoWarehouse(
                workflow=self,
                dataset_id=dataset_ids,
                serial=True,
                job_worker=self.job_workers,
                debug=self.debug)

        warehouse.setup_datasets(check_esgf=False)
        dataset_id, dataset = next(iter(warehouse.datasets.items()))
        dataset.warehouse_path = Path(data_path)

        warehouse.start_listener()

        if DatasetStatusMessage.VALIDATION_READY.value not in dataset.status:
            dataset.status = DatasetStatusMessage.VALIDATION_READY.value
        else:
            warehouse.start_datasets()

        while not warehouse.should_exit:
            sleep(2)
        print(
            f"Validation complete, dataset {dataset.dataset_id} is in state {dataset.status}")
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
