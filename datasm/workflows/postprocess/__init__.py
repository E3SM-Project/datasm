import os
import sys
from pathlib import Path
from time import sleep
from termcolor import colored, cprint
from datasm.workflows import Workflow
from datasm.util import get_dsm_paths, log_message
from datasm.dataset import DatasetStatusMessage

NAME = 'PostProcess'
COMMAND = 'postprocess'

HELP_TEXT = """
Run post-processing jobs to generate climatologies, regridded time-series, and CMIP6 datasets
"""

dsm_paths = get_dsm_paths()
default_natv_src_root = dsm_paths["PUBLICATION_DATA"]

class PostProcess(Workflow):

    def __init__(self, *args, **kwargs):
        print(" === ")
        print(f' Kwargs PostProc init: {kwargs}' )
        print(" === ", flush=True)

        super().__init__(**kwargs)

        self.name = NAME.upper()
        parallel = self.params.get('parallel')
        self.serial = False if parallel else True
        self.metadata_path = None
        log_message('info', f'initializing workflow {self.name}')

    def __call__(self):
        from datasm.datasm import AutoDataSM

        print(" === ")
        print(f' Params PostProc call: {self.params}' )
        print(" === ")

        dataset_id = self.params['dataset_id']
        log_message("info", f'Starting with datasets {dataset_id}')

        if (metadata_path := self.params.get('metadata_path')):
            self.metadata_path = Path(metadata_path)
        spec_path = self.params.get('dataset_spec') # tonyb9000
        data_path = self.params.get('data_path')
        publ_path = self.params.get('publication_path')
        natv_path = self.params.get('publication_path')
        tmpdir = self.params.get('tmp')
        status_path = self.params.get('status_path')
        testing = self.params.get('testing')

        log_message("info", f'self.params data_path = {data_path}')
        log_message("info", f'self.params publ_path = {publ_path}')
        log_message("info", f'self.params natv_path = {natv_path}')

        wh_path=self.params['warehouse_path']
        if data_path is not None:
            wh_path=data_path

        datasm = AutoDataSM(
            workflow=self,
            spec_path=spec_path,   # tonyb9000
            dataset_id=dataset_id,
            warehouse_path=wh_path,
            serial=self.serial,
            job_worker=self.job_workers,
            debug=self.debug,
            status_path=status_path,
            testing=testing,
            tmpdir=tmpdir)

        log_message("info", f"[postprocess __init__ __call__: spec_path = {spec_path}")

        datasm.setup_datasets(check_esgf=False)

        for dataset_id, dataset in datasm.datasets.items():
            dataset.warehouse_base = Path(self.params['warehouse_path'])
            if data_path:
                dataset.warehouse_path = Path(data_path)

            if DatasetStatusMessage.POSTPROCESS_READY.value not in dataset.status:
                dataset.status = DatasetStatusMessage.POSTPROCESS_READY.value

        datasm.start_listener()

        for dataset_id, dataset in datasm.datasets.items():
            datasm.start_datasets({dataset_id: dataset})

        while not datasm.should_exit:
            sleep(2)

        for dataset_id, dataset in datasm.datasets.items():
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
            '--native-srcroot',
            action="store_true",
            required=False,
            default = default_natv_src_root,
            help="The root directory to seek native data for post-processing.  Default is the publication_path (root)")
        parser.add_argument(
            '--parallel',
            action="store_true",
            help="Submit CWL workflows with the --parallel flag, to run all their steps in parallel (where possible)")
        parser.add_argument(
            '--tmp',
            required=False,
            default=f"{os.environ.get('TMPDIR', '/tmp')}",
            help=f"the directory to use for temp output, default is the $TMPDIR environment variable which you have set to: {os.environ.get('TMPDIR', '/tmp')}")
        parser = Workflow.add_args(parser)
        return COMMAND, parser

    @staticmethod
    def arg_checker(args):
        return Workflow.arg_checker(args, COMMAND)
