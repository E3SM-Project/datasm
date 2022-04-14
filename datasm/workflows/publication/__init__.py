import os
import sys
from pathlib import Path
from time import sleep
from datasm.workflows import Workflow
from datasm.dataset import Dataset, DatasetStatusMessage
from datasm.util import log_message


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
        log_message('info', f'WF_pub_init Publication_init: initializing workflow {self.name}')

    def __call__(self, *args, **kwargs):
        from datasm.datasm import AutoDataSM

        dataset_id = self.params['dataset_id']
        tmpdir = self.params['tmp']

        log_message('info', f'WF_pub_init Publication_call: starting workflow {self.name} for datasets {dataset_id}')

        if (pub_base := self.params.get('publication_path')):
            self.pub_path = Path(pub_base)
            if not self.pub_path.exists():
                log_message("info",f"WF_pub_init Publication_call: create pub dir {self.pub_path.resolve()}")
                os.makedirs(self.pub_path.resolve())
        data_path = self.params.get('data_path')
        status_path = self.params.get('status_path')

        log_message('debug', f'WF_pub_init Publication_call: supplied pub_base = {pub_base}')
        log_message('debug', f'WF_pub_init Publication_call: supplied data_path = {data_path}')
        log_message('debug', f'WF_pub_init Publication_call: supplied stat_path = {status_path}')

        # set data source root
        if data_path is not None:
            w_path=data_path
        else:
            w_path=self.params['warehouse_path']
            log_message("info", f"WF_pub_init Publication_call: AutoDataSM(workflow={self},dataset_id={dataset_id},warehouse_path={w_path},publication_path={self.pub_path},serial=True,job_workers={self.job_workers},status_path={status_path}")

        datasm = AutoDataSM(
            workflow=self,
            dataset_id=dataset_id,
            warehouse_path=w_path,
            publication_path=self.pub_path,
            serial=True,
            job_worker=self.job_workers,
            status_path=status_path,
            debug=self.debug,
            tmpdir=tmpdir)

        log_message('info', f'WF_pub_init Publication_call: issuing datasm.setup_datasets()')
        datasm.setup_datasets(check_esgf=False)

        for dataset_id, dataset in datasm.datasets.items():
            dataset.warehouse_base = Path(self.params['warehouse_path'])
            if data_path:
                dataset.warehouse_path = Path(data_path)

            if DatasetStatusMessage.PUBLICATION_READY.value not in dataset.status:
                dataset.status = DatasetStatusMessage.PUBLICATION_READY.value

        log_message('info', f'WF_pub_init Publication_call: issuing datasm.start_listener()')
        datasm.start_listener()

        for dataset_id, dataset in datasm.datasets.items():
            log_message('info', f'WF_pub_init Publication_call: calling datasm.start_datasets() starting job {self.name} for {dataset_id}')
            datasm.start_datasets({dataset_id: dataset})

        while not datasm.should_exit:
            sleep(2)

        for dataset_id, dataset in datasm.datasets.items():
            mtype = "info" if "Pass" in dataset.status else "error"
            log_message(mtype, f"WF_pub_init Publication_call: Publication complete, dataset {dataset_id} is in state {dataset.status}")

        sys.exit(0)

    @staticmethod
    def add_args(parser):
        parser = parser.add_parser(
            name=COMMAND,
            description=HELP_TEXT)
        parser = Workflow.add_args(parser)
        parser.add_argument(
            '--tmp',
            required=False,
            default=f"{os.environ.get('TMPDIR', '/tmp')}",
            help=f"the directory to use for temp output, default is the $TMPDIR environment variable which you have set to: {os.environ.get('TMPDIR', '/tmp')}")
        return COMMAND, parser

    @staticmethod
    def arg_checker(args):
        check_pass, _ = Workflow.arg_checker(args, COMMAND)
        if not check_pass:
            return False, COMMAND
        return True, COMMAND
