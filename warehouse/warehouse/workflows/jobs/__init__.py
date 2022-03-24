import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from warehouse.util import log_message


class WorkflowJob(object):

    def __init__(self, dataset, state, scripts_path, slurm_out_path, slurm_opts=[], params={}, **kwargs):
        super().__init__()
        self.name = "WorkflowJobBase"
        self._requires = { '*-*-*': None }      # dictionary where requires[req] = a_dataset (object)
        self._dataset = dataset
        self._starting_state = state
        self._scripts_path = scripts_path
        self._slurm_out = Path(slurm_out_path)
        self._slurm_opts = slurm_opts
        self._cmd = None
        self._outname = None
        self._parent = kwargs.get('parent')
        self._parameters = params
        
        workers = kwargs.get('job_workers')
        if workers is None:
            workers = 8
        self._job_workers = workers

        self._job_id = None
        self._spec_path = kwargs.get('spec_path')
        self._spec = kwargs.get('spec')
        self._config = kwargs.get('config') 
        self.debug = kwargs.get('debug')
        self.serial = kwargs.get('serial', True)
        self.tmpdir = kwargs.get('tmpdir', os.environ.get('TMPDIR', '/tmp'))
    
    def resolve_cmd(self):
        return

    def __str__(self):
        return f"{self.parent}:{self.name}:{self.dataset.dataset_id}"

    def __call__(self, slurm):
        if not self.meets_requirements():
            log_message("error", f"Job does not meet requirements! {self.requires}")
            return None
        msg = f"Starting job: {str(self)} with reqs {[x.dataset_id for x in self.requires.values()]}"
        log_message('info', msg)

        # self.resolve_cmd()
        info_fail = 0
        if self.resolve_cmd() == 1:
            log_message('error', f"bad resolve_cmd for {self.name}:{self.dataset.dataset_id}")
            info_fail = 1
            # return None [It appears that ANY return other than a valid job_id causes the warehouse to hang.] 

        working_dir = self.dataset.latest_warehouse_dir
        if self.dataset.is_locked(working_dir):
            log_message('warning', f"Cant start job working dir is locked: {working_dir}")
            return None
        else:
            self.dataset.lock(working_dir)

        self._outname = self.get_slurm_output_script_name()
        output_option = (
            '-o', f'{Path(self._slurm_out, self._outname).resolve()}')

        self._slurm_opts.extend(
            [output_option, ('-N', 1), ('-c', self._job_workers)])

        script_name = self.get_slurm_run_script_name()
        script_path = Path(self._slurm_out, script_name)
        script_path.touch(mode=0o664)

        message_file = NamedTemporaryFile(dir=self.tmpdir, delete=False)
        Path(message_file.name).touch()
        if info_fail == 1:
            self._cmd = f"export message_file={message_file.name}\n" + "\necho E2C_INFO_FAIL\n"
        else:
            self._cmd = f"export message_file={message_file.name}\n" + self._cmd

        self.add_cmd_suffix()
        log_message("info", f"WF_jobs_init:render_script: self,cmd={self.cmd}, script_path={str(script_path)}")
        slurm.render_script(self.cmd, str(script_path), self._slurm_opts)
        self._job_id = slurm.sbatch(str(script_path))
        log_message("info", f"WF_jobs_init: _call_: setting status to {self._parent}:{self.name}:Engaged: for {self.dataset.dataset_id}")
        self.dataset.status = (f"{self._parent}:{self.name}:Engaged:", 
                               {"slurm_id": self.job_id})
        return self._job_id
    
    def get_slurm_output_script_name(self):
        return f'{self.dataset.dataset_id}-{self.name}.out'

    def get_slurm_run_script_name(self):
        return f'{self.dataset.dataset_id}-{self.name}.sh'

    def add_cmd_suffix(self):
        suffix = f"""
if [ $? -ne 0 ]
then
    touch $message_file
    echo STAT:`date -u "+%Y%m%d_%H%M%S_%6N"`:{self.parent}:{self.name}:Fail:`cat $message_file` >> {self.dataset.status_path}
else
    touch $message_file
    echo STAT:`date -u "+%Y%m%d_%H%M%S_%6N"`:{self.parent}:{self.name}:Pass:`cat $message_file` >> {self.dataset.status_path}
    {self.render_cleanup()}
fi
rm $message_file
"""
        self._cmd = self._cmd + suffix

    def setup_requisites(self, input_datasets=None):
        """
        Checks that the self.dataset matches the jobs requirements, as well
        as an optional list of additional datasets
        """
        # cprint(f"incomming datasets {[x.dataset_id for x in input_datasets]}", "yellow")
        # ipdb.set_trace()

        for req, _ in self._requires.items():
            log_message("info", f"setup_requisites: DBG_REQ: self {self.name} has req {req}")

        if input_datasets == None:
            log_message("info", "setup_requisites: no input datasets")
        else:
            log_message("info", f"setup_requisites: got input datasets")

        datasets = [self.dataset]
        if input_datasets:
            if not isinstance(input_datasets, list):
                input_datasets = [input_datasets]       # turn singleton into a list
            datasets.extend(input_datasets)
            log_message("info", f"setup_requisites: incoming datasets: {[x.dataset_id for x in input_datasets]}");

        for dataset in datasets:
            # cprint(f"checking if {dataset.dataset_id} is a good match for {self.name}", "yellow")
            log_message("info", f"setup_requisites: checking if {dataset.dataset_id} is a good match for {self.name}");
            if (req := self.matches_requirement(dataset)) is not None:
                log_message("info", f"setup_requisites: self.matches_requirement(dataset) returns req = {req}");
                # log_message("info", f"setup_requisites: Found: {[x.dataset_id for x in self.requires.values()]}");
                # cprint(f'Found matching input requirements for {dataset.dataset_id}: {[x.dataset_id for x in self.requires.values()]}', 'green')
                self._requires[req] = dataset
            else:
                log_message("info", f"setup_requisites: ERR: {dataset.dataset_id} does not match for {self.requires}");
                # cprint(f'{dataset.dataset_id} does not match for {self.requires}', 'red')


    # dataset: has dataset_id, status_path, pub_base, warehouse_base, archive_base, no_status_file=True from caller, else from class
    def matches_requirement(self, dataset):
        """
        Checks that the self.dataset matches the jobs requirements, as well
        as an optional list of additional datasets
        """
        # import ipdb; ipdb.set_trace()
        # if self.dataset.dataset_id == dataset.dataset_id:
        #     return None
        for req, _ in self._requires.items():
            log_message("debug", f"matches_requirement: DBG_REQ: self {self.name} has req {req}")

        log_message("debug", f"WF_jobs_init: matches_requirement(): trying dataset.experiment={dataset.experiment}, self.dataset.experiment={self.dataset.experiment}")

        if self.dataset.project == 'E3SM':
            if dataset.experiment != self.dataset.experiment:
                return None
        else:
            dataset_facets = dataset.dataset_id.split('.')
            if self.dataset.project == 'CMIP6' and dataset.project == 'E3SM':
                e3sm_cmip_case = self._spec['project']['E3SM'][dataset_facets[1]][dataset_facets[2]].get('cmip_case')
                if not e3sm_cmip_case:
                    return None
                
                my_dataset_facets = self.dataset.dataset_id.split('.')
                my_case_attrs = '.'.join(my_dataset_facets[:5])
                if not my_case_attrs == e3sm_cmip_case:
                    return None
        
        log_message("debug", f"WF_jobs_init: matches_requirement(): Experiment ({dataset.experiment}) Aligns");

        dataset_model = dataset.model_version
        my_dataset_model = self.dataset.model_version
        if '_' in dataset_model:
            dataset_model = 'E3SM-' + '-'.join(dataset.model_version.split('_'))
        if '_' in my_dataset_model:
            my_dataset_model = 'E3SM-' + '-'.join(self.dataset.model_version.split('_'))
        if dataset_model != my_dataset_model:
            return None

        log_message("debug", f"WF_jobs_init: matches_requirement(): Model_version ({dataset_model}) Aligns");

        dataset_ensemble = dataset.ensemble
        my_dataset_ensemble = self.dataset.ensemble
        if 'ens' in dataset_ensemble:
            dataset_ensemble = f"r{dataset.ensemble[3:]}i1p1f1"
        if 'ens' in my_dataset_ensemble:
            my_dataset_ensemble = f"r{self.dataset.ensemble[3:]}i1p1f1"
        if dataset_ensemble != my_dataset_ensemble:
            return None
        
        log_message("debug", f"WF_jobs_init: matches_requirement(): Ensemble ({dataset_ensemble}) Aligns");

        for req, ds in self._requires.items():
            log_message("debug", f"WF_jobs_init: matches_requirement(): trying (req: ds) = {req}:{ds}")
            if ds:
                continue
            log_message("debug", f"WF_jobs_init: matches_requirement(): req = {req}")
            req_attrs = req.split('-')
            if len(req_attrs) > 3 and req_attrs[0] == 'sea':
                req_attrs[0] = req_attrs[0] + req_attrs[1]
                req_attrs[1] = req_attrs[2]
                req_attrs[2] = req_attrs[3]
            req = '-'.join([req_attrs[0], req_attrs[1], req_attrs[2]]) 
            log_message("info", f"WF_jobs_init: matches_requirement(): Testing realm_grid_freq {req_attrs[0]}-{req_attrs[1]}-{req_attrs[2]} against {req}")
            rcode = dataset.realm.replace('-','')
            gcode = dataset.grid.replace('-','')
            fcode = dataset.freq.replace('-','')
            if rcode != req_attrs[0] and req_attrs[0] != '*':
                continue
            if gcode != req_attrs[1] and req_attrs[1] != '*':
                continue
            if fcode != req_attrs[2] and req_attrs[2] != '*':
                continue

            log_message("info", f"WF_jobs_init: returning job requirement req={req} for dataset {dataset}")
            return req
        return None

    def meets_requirements(self):
        """
        Check if all the requirements for the job are met
        """
        for req in self._requires:
            obtained = self._requires.get(req)
            log_message("info", f"WF_jobs_init: job.meets_requirements(): checking req {req}")
            log_message("info", f"WF_jobs_init: job.meets_requirements(): self._requires.get(req) yields {obtained}")
            if not obtained:
                log_message("info", f"WF_jobs_init: job.meets_requirements(): returning False")
                return False
        return True

    def find_outpath(self):
        latest_path = self._dataset.latest_warehouse_dir
        # assuming the path ends in something like "v0" or "v0.1"
        version = latest_path.split('v')[-1]
        if '.' in version:
            version_number = int(version.split('.')[-1])
        else:
            version_number = int(version)
        new_version = version_number + 1
        return Path(Path(self._dataset.latest_warehouse_dir).parents[0], f"v0.{new_version}")
    
    def render_cleanup(self):
        return ""

    @property
    def cmd(self):
        return self._cmd

    @property
    def scripts_path(self):
        return self._scripts_path

    @property
    def starting_state(self):
        return self._starting_state

    @property
    def dataset(self):
        return self._dataset

    @property
    def requires(self):
        return self._requires

    @property
    def parent(self):
        return self._parent

    @property
    def params(self):
        return self._parameters

    @property
    def job_id(self):
        return self._job_id

    @job_id.setter
    def job_id(self, new_id):
        self._job_id = new_id
    
    @property
    def config(self):
        return self._config
