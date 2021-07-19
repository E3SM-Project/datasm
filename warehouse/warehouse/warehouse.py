import os
from re import I
import sys
from warehouse.dataset import DatasetStatusMessage
import yaml
import inspect
import fnmatch

from pprint import pformat
from pathlib import Path
from time import sleep
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from termcolor import colored, cprint


from warehouse.workflows import Workflow
from warehouse.dataset import Dataset, DatasetStatus
from warehouse.slurm import Slurm
from warehouse.listener import Listener
import warehouse.resources as resources
import warehouse.util as util
from warehouse.util import setup_logging, log_message


resource_path, _ = os.path.split(resources.__file__)
DEFAULT_SPEC_PATH = os.path.join(resource_path, "dataset_spec.yaml")
DEFAULT_CONF_PATH = os.path.join(resource_path, "warehouse_config.yaml")

with open(DEFAULT_CONF_PATH, "r") as instream:
    warehouse_conf = yaml.load(instream, Loader=yaml.SafeLoader)
DEFAULT_WAREHOUSE_PATH = warehouse_conf["DEFAULT_WAREHOUSE_PATH"]
DEFAULT_PUBLICATION_PATH = warehouse_conf["DEFAULT_PUBLICATION_PATH"]
DEFAULT_ARCHIVE_PATH = warehouse_conf["DEFAULT_ARCHIVE_PATH"]
DEFAULT_STATUS_PATH = warehouse_conf["DEFAULT_STATUS_PATH"]
NAME = "auto"

# -------------------------------------------------------------


class AutoWarehouse:
    def __init__(self, *args, **kwargs):
        super().__init__()

        self.warehouse_path = Path(kwargs.get(
            "warehouse_path", DEFAULT_WAREHOUSE_PATH))
        self.publication_path = Path(
            kwargs.get("publication_path", DEFAULT_PUBLICATION_PATH)
        )
        self.archive_path = Path(kwargs.get(
            "archive_path", DEFAULT_ARCHIVE_PATH))
        self.status_path = Path(kwargs.get("status_path", DEFAULT_STATUS_PATH))
        self.spec_path = Path(kwargs.get("spec_path", DEFAULT_SPEC_PATH))
        self.num_workers = kwargs.get("num", 8)
        self.serial = kwargs.get("serial", False)
        self.testing = kwargs.get("testing", False)
        self.dataset_ids = kwargs.get("dataset_id")
        if self.dataset_ids is not None and not isinstance(self.dataset_ids, list):
            self.dataset_ids = [self.dataset_ids]
        self.slurm_path = kwargs.get("slurm", "slurm_scripts")
        self.report_missing = kwargs.get("report_missing")
        self.job_workers = kwargs.get("job_workers", 8)
        self.datasets = None
        self.datasets_from_path = kwargs.get("datasets_from_path", False)
        os.makedirs(self.slurm_path, exist_ok=True)
        self.should_exit = False
        self.debug = kwargs.get("debug")
        self.tmpdir = kwargs.get("tmp", os.environ.get("TMPDIR", '/tmp'))

        self.scripts_path = Path(
            Path(inspect.getfile(self.__class__)).parent.absolute(), "scripts"
        ).resolve()

        # not sure where to put this - Tony
        setup_logging("debug", f"{self.slurm_path}/warehouse.log")

        if not self.report_missing:
            self.workflow = kwargs.get(
                "workflow", Workflow(
                    slurm_scripts=self.slurm_path, debug=self.debug)
            )

            self.workflow.load_children()
            self.workflow.load_transitions()

            # this is a list of WorkflowJob objects
            self.job_pool = []

            # create the local Slurm object
            self.slurm = Slurm()

            # dont setup the listener until after we've gathered the datasets
            self.listener = None

        if self.serial is True:
            log_message("info", "Running warehouse in serial mode")
        else:
            log_message(
                "info",
                f"Running warehouse in parallel mode with {self.num_workers} workers",
            )

        with open(self.spec_path, "r") as instream:
            self.dataset_spec = yaml.load(instream, Loader=yaml.SafeLoader)

    def __call__(self, check_esgf=True):
        try:
            # find missing datasets
            self.setup_datasets(check_esgf)

            if self.report_missing:
                self.print_missing()
                return 0

            self.start_listener()

            # start a workflow for each dataset as needed
            self.start_datasets()

            # wait around while jobs run
            while True:
                if self.should_exit:
                    exit(0)
                sleep(10)

        except KeyboardInterrupt:
            self.listener.stop()
            exit(1)

        return 0

    def print_missing(self):
        found_missing = False
        # import ipdb; ipdb.set_trace()
        for x in self.datasets.values():
            if x.missing:
                found_missing = True
                for m in x.missing:
                    print(f"{m}")
            elif x.status == DatasetStatus.UNITITIALIZED.value:
                found_missing = True
                msg = f"No files in dataset {x.dataset_id}"
                log_message("error", msg)
            elif x.status != DatasetStatus.SUCCESS.value:
                found_missing = True
                msg = f"Dataset {x.dataset_id} status is {x.status}"
                log_message("error", msg)
        if not found_missing:
            log_message("info", "No missing files in datasets")

    def find_e3sm_source_dataset(self, job):
        """
        Given a job with a CMIP6 dataset that needs to be run, 
        find the matching raw E3SM dataset it needs as input

        Parameters:
            job (WorkflowJob): the CMIP6 job that needs to have its requirements met

        Returns:
            Dataset, the E3SM dataset that matches the input requirements for the job if found, else None
        """
        msg = f"No raw E3SM dataset was in the list of datasets provided, seaching the warehouse for one that mathes {job}"
        log_message("debug", msg)
        for x in self.collect_e3sm_datasets():
            dataset = Dataset(
                dataset_id=x,
                status_path=os.path.join(self.status_path, f"{x}.status"),
                pub_base=self.publication_path,
                warehouse_base=self.warehouse_path,
                archive_base=self.archive_path)
            if job.matches_requirement(dataset, self.dataset_spec):
                msg = f"matching dataset found: {dataset.dataset_id}"
                log_message("debug", msg)
                return dataset
        return None

    def setup_datasets(self, check_esgf=True):
        log_message("info", "Initializing the warehouse")
        cmip6_ids = [x for x in self.collect_cmip_datasets()]
        e3sm_ids = [x for x in self.collect_e3sm_datasets()]
        all_dataset_ids = cmip6_ids + e3sm_ids

        # if the user gave us a wild card, filter out anything
        # that doesn't match their pattern

        if self.dataset_ids and self.dataset_ids is not None:
            dataset_ids = []
            for dataset_pattern in self.dataset_ids:
                if new_ids := fnmatch.filter(all_dataset_ids, dataset_pattern):
                    dataset_ids.extend(new_ids)
            self.dataset_ids = dataset_ids
        else:
            self.dataset_ids = all_dataset_ids

        if not self.dataset_ids:
            log_message(
                "error",
                f"No datasets match pattern from command line parameter --dataset-id {self.dataset_ids}",
            )
            sys.exit(1)
        else:
            msg = f"Running with datasets {pformat(self.dataset_ids)}"
            log_message('debug', msg)

        # instantiate the dataset objects with the paths to
        # where they should look for their data files
        self.datasets = {
            dataset_id: Dataset(
                dataset_id,
                status_path=os.path.join(
                    self.status_path, f"{dataset_id}.status"),
                pub_base=self.publication_path,
                warehouse_base=self.warehouse_path,
                archive_base=self.archive_path,
            )
            for dataset_id in self.dataset_ids
        }

        # fill in the start and end year for each dataset
        for dataset_id, dataset in self.datasets.items():
            if dataset.project == "CMIP6":
                start_year = self.dataset_spec["project"]["CMIP6"][dataset.activity][
                    dataset.model_version
                ][dataset.experiment]["start"]
                end_year = self.dataset_spec["project"]["CMIP6"][dataset.activity][
                    dataset.model_version
                ][dataset.experiment]["end"]
            else:
                start_year = self.dataset_spec["project"]["E3SM"][
                    dataset.model_version
                ][dataset.experiment]["start"]
                end_year = self.dataset_spec["project"]["E3SM"][dataset.model_version][
                    dataset.experiment
                ]["end"]

            dataset.start_year = start_year
            dataset.end_year = end_year

        # if the dataset is a time-series, find out what
        # its data variables are
        for dataset in self.datasets.values():
            if "time-series" in dataset.data_type:
                facets = dataset.dataset_id.split(".")
                realm_vars = self.dataset_spec["time-series"][dataset.realm]
                exclude = self.dataset_spec["project"]["E3SM"][facets[1]][
                    facets[2]
                ].get("except")
                if exclude:
                    dataset.datavars = [
                        x for x in realm_vars if x not in exclude]
                else:
                    dataset.datavars = realm_vars

        # find the state of each dataset
        if check_esgf:
            # import ipdb; ipdb.set_trace()
            if not self.serial:
                pool = ProcessPoolExecutor(max_workers=self.num_workers)
                futures = [pool.submit(x.find_status)
                           for x in self.datasets.values()]
                for future in tqdm(
                    as_completed(futures),
                    total=len(futures),
                    desc="Searching ESGF for datasets",
                ):
                    dataset_id, status, missing = future.result()
                    if isinstance(status, DatasetStatus):
                        status = status.value
                    self.datasets[dataset_id].status = status
                    self.datasets[dataset_id].missing = missing
            else:
                for dataset in tqdm(self.datasets.values()):
                    dataset_id, status, _ = dataset.find_status()
                    if isinstance(status, DatasetStatus):
                        status = status.value
                    self.datasets[dataset_id].status = status

        return

    def workflow_error(self, dataset):
        log_message(
            "error", f"Dataset {dataset.dataset_id} FAILED from {dataset.status}"
        )

    def workflow_success(self, dataset):
        log_message(
            "info", f"Dataset {dataset.dataset_id} SUCCEEDED from {dataset.status}"
        )

    def print_debug(self, msg):
        if self.debug:
            log_message("debug", msg)

    def status_was_updated(self, path):
        """
        This should be called whenever a datasets status file is updated
        Parameters: path (str) -> the path to the directory containing the status file
        """
        dataset_id = None

        with open(path, "r") as instream:
            for line in instream.readlines():
                if "DATASETID" in line:
                    dataset_id = line.split("=")[-1].strip()
        if dataset_id is None:
            log_message("error", "Unable to find dataset ID in status file")

        dataset = self.datasets[dataset_id]
        dataset.update_from_status_file()
        dataset.unlock(dataset.latest_warehouse_dir)

        # check to see of there's a slurm ID in the second to last status
        # and if there is, and the latest is either Pass or Fail, then
        # remove the job from the job_pool
        latest, second_latest = dataset.get_latest_status()
        log_message("info", f"dataset: {dataset_id} updated to state {latest}")

        if second_latest is not None:
            latest_attrs = latest.split(":")
            second_latest_attrs = second_latest.split(":")
            if "slurm_id" in second_latest_attrs[-1]:
                job_id = int(
                    second_latest_attrs[-1][second_latest_attrs[-1].index(
                        "=") + 1:]
                )
                if second_latest_attrs[-3] == latest_attrs[-3]:
                    if "Pass" in latest_attrs[-2] or "Fail" in latest_attrs[-2]:
                        for job in self.job_pool:
                            if job.job_id == job_id:
                                self.job_pool.remove(job)
                                break

        # start the transition change for the dataset
        self.start_datasets({dataset_id: dataset})

    def start_datasets(self, datasets=None):
        """
        Resolve next steps for datasets and create job objects for them
        Parameters: datasets dict of string dataset_ids to dataset objects
        Returns: list of new job objects
        """

        new_jobs = []
        ready_states = [DatasetStatus.NOT_IN_PUBLICATION.value, DatasetStatus.NOT_IN_WAREHOUSE.value,
                        DatasetStatus.PARTIAL_PUBLISHED.value, DatasetStatus.UNITITIALIZED.value]

        if datasets is None:
            datasets = self.datasets

        for dataset_id, dataset in datasets.items():
            
            if "Engaged" in dataset.status:
                continue

            # for all the datasets, if they're not yet published or in the warehouse
            # then mark them as ready to start
            if dataset.status in ready_states:
                msg = f"Dataset {dataset.dataset_id} is transitioning from {dataset.status} to {DatasetStatus.READY.value}"
                log_message('debug', msg)
                dataset.status = DatasetStatus.READY.value
                continue

            # we keep a reference to the workflow instance, so when
            # we make a job we can reconstruct the parent workflow name
            # for the status file
            params = {}
            if parameters := dataset.status.split(":")[-1].strip():
                for item in parameters.split(","):
                    key, value = item.split("=")
                    params[key] = value.replace("^", ":")

            state = dataset.status
            workflow = self.workflow

            if state == DatasetStatus.UNITITIALIZED.value:
                state = DatasetStatusMessage.WAREHOUSE_READY.value

            # check that the dataset isnt blocked by some other process thats acting on it
            # and that the workflow hasnt either failed or succeeded
            if dataset.is_blocked(state):
                msg = f"Dataset {dataset.dataset_id} at state {state} is marked as Blocked"
                log_message("error", msg)
                continue
            elif f"{self.workflow.name.upper()}:Pass:" == state:
                self.workflow_success(dataset)
                self.check_done()
                continue
            elif f"{self.workflow.name.upper()}:Fail:" == state:
                self.workflow_error(dataset)
                self.check_done()
                continue

            # there may be several transitions out of this state and
            # we need to collect them all
            engaged_states = []
            import ipdb; ipdb.set_trace()
            for item in self.workflow.next_state(dataset, state, params):
                new_state, workflow, params = item
                # if we have a new state with the "Engaged" keyword
                # we know its a leaf node that needs to be executed
                if "Engaged" in new_state:
                    engaged_states.append((new_state, workflow, params))
                # otherwise the new state and its parameters need to be
                # written to the dataset status file
                else:
                    log_message(
                        "debug", f"Dataset {dataset.dataset_id} transitioning to state {new_state} with params {params}")
                    dataset.status = (new_state, params)

            if not engaged_states:
                log_message(
                    "debug", "No jobs found to start, checking to see if all datasets are done")
                self.check_done()
                continue

            for state, workflow, params in engaged_states:
                newjob = self.workflow.get_job(
                    dataset,
                    state,
                    params,
                    self.scripts_path,
                    self.slurm_path,
                    workflow=workflow,
                    job_workers=self.job_workers,
                    spec_path=self.spec_path,
                    debug=self.debug,
                    config=warehouse_conf,
                    other_datasets=list(self.datasets.values()),
                    serial=self.serial,
                    tmpdir=self.tmpdir,
                )
                if newjob is None:
                    continue

                # check if the new job is a duplicate
                if (matching_job := self.find_matching_job(newjob)) is None:
                    self.print_debug(
                        f"Created jobs from {state} for dataset {dataset_id}"
                    )
                    new_jobs.append(newjob)
                else:
                    matching_job.setup_requisites(newjob.dataset)

        # start the jobs in the job_pool if they're ready
        for job in new_jobs:
            log_message("info", f"starting job: {job}")
            if not job.meets_requirements() and "CMIP6" in job.dataset.dataset_id:
                source_dataset = self.find_e3sm_source_dataset(job)
                if source_dataset is None:
                    msg = f"Cannot find raw input requirement for {job}"
                    log_message("error", msg)
                    continue
                job.setup_requisites(source_dataset)
            if job.job_id is None and job.meets_requirements():
                log_message(
                    "info", f"Job {job} meets its input dataset requirements")
                job_id = job(self.slurm)
                if job_id is not None:
                    job.job_id = job_id
                    self.job_pool.append(job)
                else:
                    log_message("error", f"Error starting up job {job}")
        return

    def start_listener(self):
        """
        States a file change listener for the status file
        for each of the datasets.
        """
        self.listener = []
        for _, dataset in self.datasets.items():
            log_message("info", f"starting listener for {dataset.status_path}")
            listener = Listener(warehouse=self, file_path=dataset.status_path)
            listener.start()
            self.listener.append(listener)
        log_message("info", "Listener setup complete")

    def check_done(self):
        """
        Checks all the datasets to see if they're in the Pass or Fail state,
        if ALL datasets are in either Pass or Fail, then sys.exit(0) is called
        the filesystem listeners are shut down, and the 'should_exit' variable
        is set."""
        all_done = True
        for dataset in self.datasets.values():
            if (
                f"{self.workflow.name.upper()}:Pass:" not in dataset.status
                and f"{self.workflow.name.upper()}:Fail:" not in dataset.status
            ):
                all_done = False
        if all_done:
            for listener in self.listener:
                listener.observer.stop()
            self.should_exit = True
            log_message("info", "All datasets complete, exiting")
            sys.exit(0)
        return

    def find_matching_job(self, searchjob):
        """
        Given a job object to searh for, looks at all jobs
        in the job_pool to find a job with the matching characteristics.
        Additionally checks if the job, and the searching job, meet their
        dataset input requirements.

        Parameters:
            searchjob (WorkflowJob): a job to search the job_pool for
        Returns:
            WorkflowJob: the matching job
        """
        for job in self.job_pool:
            if (
                job.name == searchjob.name
                and job.dataset.experiment == searchjob.dataset.experiment
                and job.dataset.model_version == searchjob.dataset.model_version
                and job.dataset.ensemble == searchjob.dataset.ensemble
                and not job.meets_requirements()
                and not searchjob.meets_requirements()
                and job.matches_requirement(searchjob.dataset)
                and searchjob.matches_requirement(job.dataset)
            ):
                return job
        return

    def collect_cmip_datasets(self, **kwargs):
        for activity_name, activity_val in self.dataset_spec["project"]["CMIP6"].items():
            if activity_name == "test":
                continue
            for version_name, version_value in activity_val.items():
                for experimentname, experimentvalue in version_value.items():
                    for ensemble in experimentvalue["ens"]:
                        for table_name, table_value in self.dataset_spec["tables"].items():
                            for variable in table_value:
                                if (
                                    variable in experimentvalue["except"]
                                    or table_name in experimentvalue["except"]
                                    or variable == "all"
                                ):
                                    continue
                                if "_highfreq" in variable:
                                    idx = variable.find('_')
                                    variable = variable[:idx]
                                dataset_id = f"CMIP6.{activity_name}.E3SM-Project.{version_name}.{experimentname}.{ensemble}.{table_name}.{variable}.gr"
                                yield dataset_id

    def collect_e3sm_datasets(self, **kwargs):
        for version in self.dataset_spec["project"]["E3SM"]:
            if version == "test":
                continue
            for experiment, experimentinfo in self.dataset_spec["project"]["E3SM"][
                version
            ].items():
                for ensemble in experimentinfo["ens"]:
                    for res in experimentinfo["resolution"]:
                        for comp in experimentinfo["resolution"][res]:
                            for item in experimentinfo["resolution"][res][comp]:
                                for data_type in item["data_types"]:
                                    if (
                                        item.get("except")
                                        and data_type in item["except"]
                                    ):
                                        continue
                                    dataset_id = f"E3SM.{version}.{experiment}.{res}.{comp}.{item['grid']}.{data_type}.{ensemble}"
                                    yield dataset_id

    @staticmethod
    def add_args(
        parser,
    ):
        p = parser.add_parser(name=NAME, help="Automated warehouse processing")
        p.add_argument(
            "-n", "--num", default=8, type=int, help="Number of parallel workers"
        )
        p.add_argument(
            "-s", "--serial", action="store_true", help="Run everything in serial"
        )
        p.add_argument(
            "-w",
            "--warehouse-path",
            default=DEFAULT_WAREHOUSE_PATH,
            help=f"The root path for pre-publication dataset staging, default={DEFAULT_WAREHOUSE_PATH}",
        )
        p.add_argument(
            "-p",
            "--publication-path",
            default=DEFAULT_PUBLICATION_PATH,
            help=f"The root path for data publication, default={DEFAULT_PUBLICATION_PATH}",
        )
        p.add_argument(
            "-a",
            "--archive-path",
            default=DEFAULT_ARCHIVE_PATH,
            help=f"The root path for the data archive, default={DEFAULT_ARCHIVE_PATH}",
        )
        p.add_argument(
            "--dataset-id",
            nargs="*",
            help="Run the automated processing for the given datasets, this can the the complete dataset_id, "
            "or a glob such as E3SM.1_0.*.time-series. or CMIP6.*.Amon. the default is to run on all CMIP6 "
            "and  E3SM project datasets",
        )
        p.add_argument(
            "--warehouse-config",
            default=DEFAULT_CONF_PATH,
            help="The default warehouse/publication/archives paths are drawn from a config yaml file "
            "you can change the values via the command line or change the contents of the file here "
            f"{DEFAULT_CONF_PATH}",
        )
        p.add_argument(
            "--dataset-spec",
            default=DEFAULT_SPEC_PATH,
            help=f"The path to the dataset specification yaml file, default={DEFAULT_SPEC_PATH}",
        )
        p.add_argument(
            "--status-path",
            default=DEFAULT_STATUS_PATH,
            help=f"The path to where to store dataset status files, default={DEFAULT_STATUS_PATH}",
        )
        p.add_argument(
            "--job-workers",
            type=int,
            default=8,
            help="number of parallel workers each job should create when running, default=8",
        )
        p.add_argument(
            "--testing", action="store_true", help="run the warehouse in testing mode"
        )
        p.add_argument(
            "--slurm-path",
            required=False,
            default="slurm_scripts",
            help=f'The directory to hold slurm batch scripts as well as console output from batch jobs, default={os.environ["PWD"]}/slurm_scripts',
        )
        p.add_argument(
            "--tmp",
            required=False,
            default=f"{os.environ.get('TMPDIR', '/tmp')}",
            help=f"the directory to use for temp output, default is the $TMPDIR environment variable which you have set to: {os.environ.get('TMPDIR', '/tmp')}",
        )
        p.add_argument(
            "--report-missing",
            required=False,
            action="store_true",
            help="After collecting the datasets, print out any that have missing files and exit",
        )
        p.add_argument(
            "--debug",
            action="store_true",
            help="Print additional debug information to the console",
        )
        return NAME, parser

    @staticmethod
    def arg_checker(args):
        return True, NAME
