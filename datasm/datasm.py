"""The datasm module."""
import fnmatch
import inspect
import os
import sys
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from pprint import pformat
from re import I
from time import sleep

import ipdb
import yaml
from termcolor import colored, cprint
from tqdm import tqdm

import datasm.resources as resources
import datasm.util as util
from datasm.dataset import Dataset, DatasetStatus, DatasetStatusMessage
from datasm.listener import Listener
from datasm.slurm import Slurm
from datasm.util import get_dsm_paths, log_message, setup_logging, parent_native_dsid
from datasm.workflows import Workflow


dsm_paths = get_dsm_paths()
outer_resource_path = dsm_paths["STAGING_RESOURCE"]
DEFAULT_SPEC_PATH = os.path.join(outer_resource_path, "dataset_spec.yaml")
DEFAULT_STATUS_PATH = dsm_paths["STAGING_STATUS"]
DEFAULT_WAREHOUSE_PATH = dsm_paths["STAGING_DATA"]
DEFAULT_PUBLICATION_PATH = dsm_paths["PUBLICATION_DATA"]
DEFAULT_ARCHIVE_PATH = dsm_paths["ARCHIVE_STORAGE"]

inner_resource_path, _ = os.path.split(resources.__file__)
DEFAULT_CONF_PATH = os.path.join(inner_resource_path, "datasm_config.yaml")

with open(DEFAULT_CONF_PATH, "r") as instream:
    datasm_conf = yaml.load(instream, Loader=yaml.SafeLoader)
NAME = "auto"

# -------------------------------------------------------------


class AutoDataSM:
    def __init__(self, *args, **kwargs):
        super().__init__()

        global DEFAULT_SPEC_PATH

        self.slurm_path = kwargs.get("slurm", "slurm_scripts")
        # not sure where to put this - Tony
        setup_logging("debug", f"{self.slurm_path}/datasm.log")

        self.warehouse_path = Path(kwargs.get( "warehouse_path", DEFAULT_WAREHOUSE_PATH))
        self.publication_path = Path( kwargs.get("publication_path", DEFAULT_PUBLICATION_PATH))
        # Did we really get the command line publication path here?

        self.archive_path = Path(kwargs.get( "archive_path", DEFAULT_ARCHIVE_PATH))
        self.status_path = Path(kwargs.get("status_path", DEFAULT_STATUS_PATH))

        self.spec_path = Path(kwargs.get("spec_path", DEFAULT_SPEC_PATH))

        if self.spec_path != DEFAULT_SPEC_PATH:
            DEFAULT_SPEC_PATH = Path(self.spec_path)
            log_message("info", f"TOP: Applying alternative dataset_spec: {self.spec_path}")
        

        self.num_workers = kwargs.get("num", 8)
        self.serial = kwargs.get("serial", False)
        self.testing = kwargs.get("testing", False)
        self.dataset_ids = kwargs.get("dataset_id")
        if self.dataset_ids is not None and not isinstance(self.dataset_ids, list):
            self.dataset_ids = [self.dataset_ids]
        self.report_missing = kwargs.get("report_missing")
        self.job_workers = kwargs.get("job_workers", 8)
        self.datasets = None
        self.datasets_from_path = kwargs.get("datasets_from_path", False)
        os.makedirs(self.slurm_path, exist_ok=True)
        self.should_exit = False

        if kwargs.get("debug"):
            self.debug = "DEBUG"
        else:
            self.debug = "INFO"

        self.ask = kwargs.get("ask")
        self.tmpdir = kwargs.get("tmp", os.environ.get("TMPDIR", '/tmp'))

        self.scripts_path = Path(
            Path(inspect.getfile(self.__class__)).parent.absolute(), "scripts"
        ).resolve()

        log_message("info", f"TOP: self.warehouse_path = {self.warehouse_path}")
        log_message("info", f"TOP: self.publication_path = {self.publication_path}")
        log_message("info", f"TOP: self.slurm_path = {self.slurm_path}")
        log_message("info", f"TOP: self.spec_path = {self.spec_path}")

        if self.report_missing:
            pass
        else:
            self.workflow = kwargs.get(
                "workflow", Workflow(
                    slurm_scripts=self.slurm_path,
                    debug=self.debug,
                    job_workers=self.job_workers)
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
            log_message("info", "Running datasm in serial mode")
        else:
            log_message("info", f"Running datasm in parallel mode with {self.num_workers} workers")

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
                    sys.exit(0)
                sleep(10)

        except KeyboardInterrupt:
            if listeners := self.listener:
                for l in listeners:
                    l.stop()
            sys.exit(1)

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

    '''
    The find_e3sm_source_dataset function will take each E3SM dataset_id, instantiate a "Dataset" object that breaks out
    the component facets (project, experiment, etc) and then passes that Dataset object to the "job.requires_dataset()"
    function defined in workflows/__init__.py to test if it matches the requirements for the job.

    If a dataset matches a requirement, that dataset is returned to the caller, else None is returned.
    '''

    def find_e3sm_source_dataset(self, job):
        """
        Given a job with a CMIP6 dataset that needs to be run,
        find the matching raw E3SM dataset it needs as input

        Parameters:
            job (WorkflowJob): the CMIP6 job that needs to have its requirements met

        Returns:
            Dataset, the E3SM dataset that matches the input requirements for the job if found, else None
        """
        # log_message("debug", f"No raw E3SM dataset was in the list of datasets provided, seaching the warehouse for one that mathes {job}")
        log_message("info", f"find_e3sm_source_dataset: Seeking raw E3SM dataset for job {job.name} (type(job) = {type(job)})")

        log_message("debug", f"job._dataset = {job._dataset}")
        log_message("debug", f"job.dataset = {job.dataset}")

        native_dsid = parent_native_dsid(job.dataset.dataset_id)
        log_message("info", f"{__name__}: find_e3sm_source_dataset: parent_native_dsid() returns {native_dsid}")

        if native_dsid == "None":
            log_message("error", f"{__name__}: find_e3sm_source_dataset: Found no native_dsid for job {job.name}")
            return None

        for req, ds in job._requires.items(): # located in workflows/jobs/__init__.py
            log_message("info", f"{__name__}: find_e3sm_source_dataset: DEBUG: job._requires includes req = {req}, ds = {ds}")

        dataset = Dataset(
            dataset_id=native_dsid,
            status_path=os.path.join(self.status_path, f"{native_dsid}.status"),
            pub_base=self.publication_path,
            warehouse_base=self.warehouse_path,
            archive_base=self.archive_path,
            no_status_file=True)

        log_message("info", f"{__name__}: find_e3sm_source_dataset: tries dsid {dataset.dataset_id}, calls 'requires_dataset()'")

        if job.requires_dataset(dataset):
            log_message("info", f"{__name__}: find_e3sm_source_dataset: dataset {dataset.dataset_id} matched job requirement")
            dataset.initialize_status_file()
            # log_message("debug", msg, self.debug)
            log_message("info", f"{__name__}: find_e3sm_source_dataset: Found {dataset.dataset_id} for job {job.name}")
            return dataset

        log_message("error", f"{__name__}: find_e3sm_source_dataset: Found None for job {job.name}")
        return None

    def setup_datasets(self, check_esgf=True):
        cmip6_ids = [x for x in self.collect_cmip_datasets()]
        e3sm_ids = [x for x in self.collect_e3sm_datasets()]
        all_dataset_ids = cmip6_ids + e3sm_ids

        # if the user gave us a wild card, filter out anything
        # that doesn't match their pattern

        if self.dataset_ids and self.dataset_ids is not None:
            dataset_ids = []
            for dataset_pattern in self.dataset_ids:
                log_message("info", f"setup_datasets: testing for pattern {dataset_pattern} in all_dataset_ids")
                new_ids = fnmatch.filter(all_dataset_ids, dataset_pattern)
                log_message("info", f"setup_datasets: matches are: {new_ids}")
                if new_ids := fnmatch.filter(all_dataset_ids, dataset_pattern):
                    dataset_ids.extend(new_ids)
            self.dataset_ids = dataset_ids
        else:
            self.dataset_ids = all_dataset_ids

        if not self.dataset_ids:

            log_message( "info", f"setup_datasets: No datasets in dataset_spec ({self.spec_path}) match pattern from command line parameter --dataset-id {self.dataset_ids}")
            sys.exit(1)
            # os._exit(1)
        else:
            msg = f"Running with datasets {pformat(self.dataset_ids)}"
            log_message('debug', msg)
            log_message("info", f"setup_datasets: Running with {len(self.dataset_ids)} datasets")

        # instantiate the dataset objects with the paths to where they should look for their status and data files.
        # Calls dataset.py __init__() on each dataset_id.  Note: application of user-supplied path_roots is enforced here.
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
                start_year = self.dataset_spec["project"]["CMIP6"][dataset.activity][dataset.institution][dataset.model_version][dataset.experiment]["start"]
                end_year = self.dataset_spec["project"]["CMIP6"][dataset.activity][dataset.institution][dataset.model_version][dataset.experiment]["end"]
            else:
                start_year = self.dataset_spec["project"]["E3SM"][dataset.model_version][dataset.experiment]["start"]
                end_year = self.dataset_spec["project"]["E3SM"][dataset.model_version][dataset.experiment]["end"]

            dataset.start_year = start_year
            dataset.end_year = end_year

        # if the dataset is a time-series, find out what
        # its data variables are [must have project=E3SM]
        for dataset in self.datasets.values():
            if "time-series" in dataset.data_type:
                facets = dataset.dataset_id.split(".")
                realm_vars = self.dataset_spec["time-series"][dataset.realm]
                exclude = self.dataset_spec["project"]["E3SM"][facets[1]][facets[2]].get("except")
                if exclude:
                    dataset.datavars = [x for x in realm_vars if x not in exclude]
                else:
                    dataset.datavars = realm_vars

        # find the state of each dataset
        if check_esgf:
            # import ipdb; ipdb.set_trace()
            if not self.serial:
                pool = ProcessPoolExecutor(max_workers=self.num_workers)
                futures = [pool.submit(x.find_status) for x in self.datasets.values()]
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

        if self.ask:
            for name, dataset in self.datasets.items():
                print(f"{name} is in state {dataset.status}")
            if input("Proceed? y/[n]\n") != 'y':
                self.should_exit = True
                sys.exit(0)

        log_message("info", f"setup_datasets: RETURN")

        return

    def workflow_error(self, dataset):
        log_message(
            "error", f"Dataset {dataset.dataset_id} FAILED from {dataset.status}"
        )

    def workflow_success(self, dataset):
        log_message(
            "info", f"Dataset {dataset.dataset_id} SUCCEEDED from {dataset.status}"
        )

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
            log_message("error", "setup_datasets: Unable to find dataset ID in status file")

        dataset = self.datasets[dataset_id]
        dataset.update_from_status_file()
        dataset.unlock(dataset.latest_warehouse_dir)

        # check to see of there's a slurm ID in the second to last status
        # and if there is, and the latest is either Pass or Fail, then
        # remove the job from the job_pool
        latest, second_latest = dataset.get_latest_status()
        log_message("info", f"status_was_updated: {dataset_id} updated to state {latest}")

        if second_latest is not None:
            latest_attrs = latest.split(":")
            second_latest_attrs = second_latest.split(":")
            if "slurm_id" in second_latest_attrs[-1]:
                job_id = int(
                    second_latest_attrs[-1][second_latest_attrs[-1].index(
                        "=") + 1:]
                )
                # if the job names are the same
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

        Exit_On_Bad_Job = True

        log_message("info", f"start_datasets: Generate job objects for each dataset")
        log_message("debug", f"start_datasets: datasets={datasets}")
        new_jobs = []
        ready_states = [DatasetStatus.NOT_IN_PUBLICATION.value, DatasetStatus.NOT_IN_WAREHOUSE.value,
                        DatasetStatus.PARTIAL_PUBLISHED.value, DatasetStatus.UNITITIALIZED.value]

        ''' DBG
        rsm = ""
        for stateval in ready_states:
            rsm = rsm + f"{stateval},"
        log_message("info", f"start_datasets: ready_states include {rsm}")
        end DBG '''

        if datasets is None:
            datasets = self.datasets

        for dataset_id, dataset in datasets.items():

            log_message("debug", f"start_datasets: working datasets_id {dataset_id} from datasets.items()")

            if "Engaged" in dataset.status:
                log_message("debug", f"start_datasets: 'Engaged' in dataset.status: continue")
                continue

            # for all the datasets, if they're not yet published or in the warehouse
            # then mark them as ready to start
            if dataset.status in ready_states:
                log_message('info', f"start_datasets: Dataset {dataset.dataset_id} is transitioning from {dataset.status} to {DatasetStatus.READY.value}")
                dataset.status = DatasetStatus.READY.value
                continue

            # import ipdb; ipdb.set_trace()
            # we keep a reference to the workflow instance, so when
            # we make a job we can reconstruct the parent workflow name
            # for the status file
            log_message("info", f"start_datasets: To reconstruct parent workflow name:")
            params = {}
            if parameters := dataset.status.split(":")[-1].strip():
                for item in parameters.split(","):
                    if len(item.split("=")) > 1:
                        key, value = item.split("=")
                        params[key] = value.replace("^", ":")
                        log_message("debug", f"start_datasets: params[{key}] = {params[key]}")

            state = dataset.status
            workflow = self.workflow

            log_message("info", f"start_datasets: state = {state}")
            log_message("info", f"start_datasets: workflow = {workflow}")

            if state == DatasetStatus.UNITITIALIZED.value:
                state = DatasetStatusMessage.WAREHOUSE_READY.value

            log_message("info", f"self.workflow.name.upper() = {self.workflow.name.upper()}")
            log_message("info", f"dataset.status = {dataset.status}")

            # check that the dataset isnt blocked by some other process thats acting on it
            # and that the workflow hasnt either failed or succeeded

            # import ipdb; ipdb.set_trace()
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
            for item in self.workflow.next_state(dataset, state, params):

                log_message("debug",f"type(item) = {type(item)}")

                new_state, workflow, params = item

                log_message("debug",f"  type(new_state) = {type(new_state)}")
                log_message("debug",f"  type(workflow) = {type(workflow)}")
                log_message("debug",f"  type(params) = {type(params)}")

                # for spurious/unexpected STAT: messages
                if new_state == "no_state_change":
                    continue
 
                # if we have a new state with the "Engaged" keyword
                # we know its a leaf node that needs to be executed
                if "Engaged" in new_state:
                    engaged_states.append((new_state, workflow, params))
                # otherwise the new state and its parameters need to be
                # written to the dataset status file
                else:
                    msg = f"start_datasets: Dataset {dataset.dataset_id} transitioning to state {new_state}"
                    if params:
                        msg += f" with params {params}"
                    log_message("info", msg)
                    log_message("debug", msg, self.debug)
                    dataset.status = (new_state, params)

            if not engaged_states:
                continue

            for state, workflow, params in engaged_states:
                # import ipdb; ipdb.set_trace()
                # Triggers workflow __init__ get_job to call job init
                log_message("info", f"start_datasets: instantiating newjob = self.workflow.get_job() for dataset {dataset.dataset_id}")
                newjob = self.workflow.get_job(
                    dataset,
                    state,
                    params,
                    self.scripts_path,
                    self.slurm_path,
                    workflow=workflow,
                    job_workers=self.job_workers,
                    spec=self.dataset_spec,
                    debug=self.debug,
                    config=datasm_conf,
                    other_datasets=list(self.datasets.values()),
                    serial=self.serial,
                    tmpdir=self.tmpdir,
                )
                if newjob is None:
                    continue

                # check if the new job is a duplicate
                if (matching_job := self.find_matching_job(newjob)) is None:
                    log_message("info", f"start_datasets: Adding job {newjob.name} for dataset {dataset_id} in state {state}")
                    new_jobs.append(newjob)
                else:
                    log_message("info", f"start_datasets: Found job {matching_job.name} for dataset {dataset_id} in state {state}, calling setup_requisites for new job {newjob.name} and dataset {newjob.dataset.dataset_id}")
                    matching_job.setup_requisites(newjob.dataset)

        zpasses = 0
        # start the jobs in the job_pool if they're ready
        for job in new_jobs:
            zpasses = zpasses + 1
            log_message("info", f"job type = {type(job)}")
            job_name = f"{job}".split(':')[0]
            job_name2 = f"{job}".split(':')[1]
            log_message("info", f"start_datasets: starting job: {job_name}:{job_name2}")
            # import ipdb; ipdb.set_trace()
            job_reqs_met = job.meets_requirements()
            log_message("info", f"start_datasets: job_reqs_met={job_reqs_met}, project={job.dataset.project}, job_name={job_name}")
            if not job_reqs_met and job.dataset.project == "CMIP6" or ( job_name == 'POSTPROCESS' and ('time-series' in job.dataset.dataset_id or 'climo' in job.dataset.dataset_id) ):
                log_message("info", f"start_datasets: Calling self.find_e3sm_source_dataset(job) for job {job_name}")
                source_dataset = self.find_e3sm_source_dataset(job)
                log_message("info", f"start_datasets: Returns self.find_e3sm_source_dataset(job) for job {job_name}")
                if source_dataset is None:
                    msg = f"Cannot find raw input requirement for job {job}. source_dataset is None from self.find_e3sm_source_dataset(job)"
                    log_message("error", msg)
                    continue
                log_message("info", f"start_datasets: found E3SM source dataset: {source_dataset.dataset_id}")
                log_message("info", f"start_datasets: calling setup_requisites : {source_dataset.dataset_id}")
                job.setup_requisites(source_dataset)
                job_reqs_met = job.meets_requirements()
                log_message("info", f"start_datasets: job_reqs_met={job_reqs_met}") # True/False
            if job.job_id is None and job_reqs_met:
                log_message("info", f"start_datasets: (job.job_id={job.job_id}) Job {job_name} meets its input dataset requirements, calling job(self.slurm)")
                job_id = job(self.slurm)        # WARNING:  If job script exits prematurely, this call never returns and datasm hangs.
                log_message("info", f"start_datasets: DGB: got job_id {job_id} from job(self.slurm)")
                if job_id is not None:
                    log_message("info",f"Adding job with job_id {job_id} to self.job_pool")
                    job.job_id = job_id
                    self.job_pool.append(job)
                else:
                    log_message("error", f"Error starting up job {job}. EXIT if serial.")
                    if Exit_On_Bad_Job and self.serial:
                        os._exit(1)
                    continue
            else:
                log_message("error", "DGB: job NOT added to pool. EXIT if serial")
                if Exit_On_Bad_Job and self.serial:
                    os._exit(1)
                log_message("info", f"start_datasets: DGB: job.job_id = {job.job_id}, job_reqs_met = {job_reqs_met}")
                attributes = [attr for attr in dir(job) if not attr.startswith('__')]
                for attr in attributes:
                    print(f"{attr} = {getattr(job,attr)}")
            log_message("info", f"start_datasets: (bottom loop: for job in new_jobs)")
        job_pool_size = len(self.job_pool)
        if len(self.job_pool) == 0:
            log_message("info", f"Job Pool contains No jobs, pass={zpasses}:  datasm NOT exiting.")
            # sys.exit(0)
        log_message("info", f"start_datasets: Return: Job Pool contains {job_pool_size} jobs, pass={zpasses}") # try to exit if no jobs
        return

    def start_listener(self):
        """
        Starts a file change listener for the status file
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
        log_message("info", "find_matching_job: searching all job in self.job_pool")
        for job in self.job_pool:
            log_message("info", f"find_matching_job:     trying job {job.name} against searchjob {searchjob.name}")
            if (
                job.name == searchjob.name
                and job.dataset.experiment == searchjob.dataset.experiment
                and job.dataset.model_version == searchjob.dataset.model_version
                and job.dataset.ensemble == searchjob.dataset.ensemble
                and not job.meets_requirements()
                and not searchjob.meets_requirements()
                and job.requires_dataset(searchjob.dataset)
                and searchjob.requires_dataset(job.dataset)
            ):
                log_message("info", f"find_matching_job:     Returning job {job.name}")
                return job
        log_message("info", f"find_matching_job:     Returning None")
        return

    def collect_cmip_datasets(self, **kwargs):

        for activity_name, activity_val in self.dataset_spec["project"]["CMIP6"].items():
            if activity_name == "test" and not self.testing:
                continue
            for institution_id, institution_branch in activity_val.items():
                for version_name, version_value in institution_branch.items():    # version_name is CMIP6 Source_ID
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
                                    dataset_id = f"CMIP6.{activity_name}.{institution_id}.{version_name}.{experimentname}.{ensemble}.{table_name}.{variable}.gr"
                                    yield dataset_id

    def collect_e3sm_datasets(self, **kwargs):
        for version in self.dataset_spec["project"]["E3SM"]:
            if version == "test" and not self.testing:
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
        p = parser.add_parser(name=NAME, help="Automated dataset processing")
        p.add_argument(
            "-n", "--num", default=8, type=int, help="Number of parallel workers"
        )
        p.add_argument(
            "-s", "--serial", action="store_true", help="Run esgf checks in serial"
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
            "-d", "--dataset-id",
            nargs="*",
            help="Run the automated processing for the given datasets, this can the the complete dataset_id, "
            "or a glob such as E3SM.1_0.*.time-series. or CMIP6.*.Amon. the default is to run on all CMIP6 "
            "and  E3SM project datasets",
        )
        p.add_argument(
            "--datasm-config",
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
            "--testing", action="store_true", help="run the datasm in testing mode"
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
            "--ask",
            required=False,
            action="store_true",
            help=f"When starting up, print out the datasets that will be affected (and their initial status), and ask the user if they would like to proceed.",
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
