import sys
from pathlib import Path
from time import sleep
from warehouse.workflows import Workflow
from warehouse.dataset import Dataset, DatasetStatusMessage


NAME = 'Validation'
COMMAND = 'validate'


class Validation(Workflow):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.name = NAME.upper()
        self.datasets = None
        self.job_workers = kwargs.get('job_workers')
        

    def __call__(self, *args, **kwargs):
        from warehouse.warehouse import AutoWarehouse

        dataset_ids = self.params['dataset_id']

        if (data_path := self.params.get('data_path')):
            warehouse = AutoWarehouse(
                workflow=self,
                dataset_id=dataset_ids,
                warehouse_path=data_path,
                serial=True,
                job_worker=self.job_workers)
        else:
            warehouse = AutoWarehouse(
                workflow=self,
                dataset_id=dataset_ids,
                serial=True,
                job_worker=self.job_workers)
        
        warehouse.setup_datasets(check_esgf=False)
        dataset_id, dataset = next(iter(warehouse.datasets.items()))
        dataset.warehouse_path = Path(data_path)
        # import ipdb; ipdb.set_trace()
        if DatasetStatusMessage.VALIDATION_READY.value not in dataset.status:
            dataset.update_status(DatasetStatusMessage.VALIDATION_READY.value)
        else:
            warehouse.start_datasets()

        while True:
            if warehouse.should_exit:
                sys.exit(0)
            sleep(10)


    @staticmethod
    def add_args(parser):
        parser = parser.add_parser(
            name=COMMAND,
            help='validate a raw dataset')
        parser = Workflow.add_args(parser)
        return COMMAND, parser

    @staticmethod
    def arg_checker(args):
        return Workflow.arg_checker(args, COMMAND)

