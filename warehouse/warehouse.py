import os
import yaml
import json
from pathlib import Path

from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

from esgfpub import resources
from warehouse.workflows import Workflow
from warehouse.dataset import Dataset, DatasetStatus

DEFAULT_WAREHOUSE_PATH = '/p/user_pub/e3sm/warehouse/'
DEFAULT_PUBLICATION_PATH = '/p/user_pub/work/'
DEFAULT_ARCHIVE_PATH = '/p/user_pub/e3sm/archive'

resource_path, _ = os.path.split(resources.__file__)
DEFAULT_SPEC_PATH = os.path.join(resource_path, 'dataset_spec.yaml')
NAME = 'auto'


class AutoWarehouse():

    def __init__(self, *args, **kwargs):
        super().__init__()

        self.warehouse_path = Path(kwargs.get(
            'warehouse_path', DEFAULT_WAREHOUSE_PATH))
        self.publication_path = Path(kwargs.get(
            'publication_path', DEFAULT_PUBLICATION_PATH))
        self.archive_path = Path(kwargs.get(
            'archive_path', DEFAULT_ARCHIVE_PATH))
        self.spec_path = Path(kwargs.get('spec_path', DEFAULT_SPEC_PATH))
        self.sproket_path = kwargs.get('sproket', 'sproket')
        self.num_workers = kwargs.get('num', 8)
        self.serial = kwargs.get('serial', False)
        self.testing = kwargs.get('testing', False)
        self.dataset_ids = kwargs.get('dataset_id', False)
        self.sproket_path = kwargs.get('sproket', 'sproket')

        self.workflow = Workflow()
        self.workflow.load_children()
        self.workflow.load_transitions()

        if self.serial:
            print("Running warehouse in serial mode")
        else:
            print(
                f"Running warehouse in parallel mode with {self.num_workers} workers")

        with open(self.spec_path, 'r') as instream:
            self.dataset_spec = yaml.load(instream, Loader=yaml.SafeLoader)

    def __call__(self):
        # find missing datasets
        print("Initializing the warehouse")

        cmip6_ids = [x for x in self.collect_cmip_datasets()]
        if self.testing:
            cmip6_ids = cmip6_ids[:100]
        e3sm_ids = [x for x in self.collect_e3sm_datasets()]
        if self.testing:
            e3sm_ids = e3sm_ids[:100]
        dataset_ids = cmip6_ids + e3sm_ids

        # if the user gave us a wild card, filter out anything
        # that doesn't match their pattern
        if self.dataset_ids:
            ndataset_ids = []
            for i in dataset_ids:
                found = False
                for ii in self.dataset_ids:
                    if ii in i:
                        found = True
                        break
                if found:
                    ndataset_ids.append(i)
            dataset_ids = ndataset_ids

        datasets = {
            dataset_id: Dataset(
                dataset_id,
                pub_base=self.publication_path,
                warehouse_base=self.warehouse_path,
                archive_base=self.archive_path,
                sproket=self.sproket_path)
            for dataset_id in dataset_ids
        }

        # fill in the start and end year if possible
        for dataset_id, dataset in datasets.items():
            if dataset.project == 'CMIP':
                start_year = self.dataset_spec['project']['CMIP'][dataset.activity][
                    dataset.model_version][dataset.experiment]['start']
                end_year = self.dataset_spec['project']['CMIP'][dataset.activity][
                    dataset.model_version][dataset.experiment]['end']
            else:
                start_year = self.dataset_spec['project']['E3SM'][dataset.model_version][dataset.experiment]['start']
                end_year = self.dataset_spec['project']['E3SM'][dataset.model_version][dataset.experiment]['end']
            dataset.start_year = start_year
            dataset.end_year = end_year

        for dataset in datasets.values():
            if 'time-series' in dataset.data_type:
                facets = dataset.dataset_id.split('.')
                realm_vars = self.dataset_spec['time-series'][dataset.realm]
                exclude = self.dataset_spec['project']['E3SM'][facets[1]][facets[2]].get(
                    'except')
                if exclude:
                    dataset.datavars = [
                        x for x in realm_vars if x not in exclude]
                else:
                    dataset.datavars = realm_vars

        # find the state of each dataset
        dataset_status = {}
        if not self.serial:
            pool = ProcessPoolExecutor(max_workers=self.num_workers)
            futures = [pool.submit(x.find_status) for x in datasets.values()]

            for future in tqdm(as_completed(futures), total=len(futures), desc="Searching ESGF for datasets"):
                res = future.result()
                dataset_id, status = res
                dataset_status[dataset_id] = status
        else:

            for dataset in tqdm(datasets.values()):
                dataset_id, status = dataset.find_status()
                dataset_status[dataset_id] = status

        import ipdb
        ipdb.set_trace()
        from pprint import pprint
        pprint(dataset_status)

        # start a workflow for each dataset (if needed)
        for dataset_id, status in dataset_status.items():
            if status == DatasetStatus.SUCCESS:
                continue
            ...

        return 0

    def nextState(self, dataset):
        ...

    def collect_cmip_datasets(self, **kwargs):
        for activity_name, activity_val in self.dataset_spec['project']['CMIP'].items():
            for version_name, version_value in activity_val.items():
                for experimentname, experimentvalue in version_value.items():
                    for ensemble in experimentvalue['ens']:
                        for table_name, table_value in self.dataset_spec['tables'].items():
                            for variable in table_value:
                                if variable in experimentvalue['except']:
                                    continue
                                dataset_id = f"CMIP.{activity_name}.E3SM-Project.{version_name}.{experimentname}.{ensemble}.{table_name}.{variable}.gr.*"
                                yield dataset_id

    def collect_e3sm_datasets(self, **kwargs):
        for version in self.dataset_spec['project']['E3SM']:
            for experiment, experimentinfo in self.dataset_spec['project']['E3SM'][version].items():
                for ensemble in experimentinfo['ens']:
                    for res in experimentinfo['resolution']:
                        for comp in experimentinfo['resolution'][res]:
                            for item in experimentinfo['resolution'][res][comp]:
                                for data_type in item['data_types']:
                                    if item.get('except') and data_type in item['except']:
                                        continue
                                    dataset_id = f"E3SM.{version}.{experiment}.{res}.{comp}.{item['grid']}.{data_type}.{ensemble}.*"
                                    yield dataset_id

    @staticmethod
    def add_args(parser):
        p = parser.add_parser(
            name=NAME,
            help="Automated warehouse processing")
        p.add_argument(
            '-n', '--num',
            default=8,
            type=int,
            help="Number of parallel workers")
        p.add_argument(
            '-s', '--serial',
            action="store_true",
            help="Run everything in serial")
        p.add_argument(
            '-w', '--warehouse-path',
            default=DEFAULT_WAREHOUSE_PATH,
            help="The root path for pre-publication dataset staging")
        p.add_argument(
            '-p', '--publication-path',
            default=DEFAULT_PUBLICATION_PATH,
            help="The root path for data publication")
        p.add_argument(
            '-a', '--archive-path',
            default=DEFAULT_ARCHIVE_PATH,
            help="The root path for the data archive")
        p.add_argument(
            '-d', '--dataset-spec',
            default=DEFAULT_SPEC_PATH,
            help='The path to the dataset specification yaml file')
        p.add_argument(
            '--dataset-id',
            nargs='*',
            help='Only run the automated processing for the given datasets, this can the the complete dataset_id, '
                 'or a wildcard such as E3SM.1_0.')
        p.add_argument(
            '--testing',
            action="store_true",
            help='run the warehouse in testing mode')
        p.add_argument(
            '--sproket',
            required=False,
            default='sproket',
            help='path to sproket binary if its not in your $PATH')
        return NAME, parser

    @staticmethod
    def arg_checker(args):
        return True, NAME

    def get_dataset_spec(self):
        with open(self.spec_path, 'r') as ip:
            return yaml.safe_load(ip)
