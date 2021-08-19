import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from warehouse.util import log_message


class WorkflowJob(object):

    def __init__(self, dataset, state, scripts_path, slurm_out_path, slurm_opts=[], params={}, **kwargs):
        super().__init__()
        self.name = "WorkflowJobBase"
        self._requires = { '*-*-*': None }
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
        log_message('debug', msg)

        self.resolve_cmd()

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
        self._cmd = f"export message_file={message_file.name}\n" + self._cmd

        self.add_cmd_suffix()
        slurm.render_script(self.cmd, str(script_path), self._slurm_opts)
        self._job_id = slurm.sbatch(str(script_path))
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
    echo STAT:`date "+%Y/%m/%d %H.%M.%S.%6N"`:{self.parent}:{self.name}:Fail:`cat $message_file` >> {self.dataset.status_path}
else
    touch $message_file
    echo STAT:`date "+%Y/%m/%d %H.%M.%S.%6N"`:{self.parent}:{self.name}:Pass:`cat $message_file` >> {self.dataset.status_path}
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
        datasets = [self.dataset]
        if input_datasets:
            if not isinstance(input_datasets, list):
                input_datasets = [input_datasets]
            datasets.extend(input_datasets)

        for dataset in datasets:
            # cprint(f"checking if {dataset.dataset_id} is a good match for {self.name}", "yellow")
            if (req := self.matches_requirement(dataset)) is not None:
                # cprint(f'Found matching input requirements for {dataset.dataset_id}: {[x.dataset_id for x in self.requires.values()]}', 'green')
                self._requires[req] = dataset
            # else:
            #     cprint(f'{dataset.dataset_id} does not match for {self.requires}', 'red')

    def matches_requirement(self, dataset):
        """
        Checks that the self.dataset matches the jobs requirements, as well
        as an optional list of additional datasets
        """
        # import ipdb; ipdb.set_trace()
        # if self.dataset.dataset_id == dataset.dataset_id:
        #     return None

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
        
        dataset_model = dataset.model_version
        my_dataset_model = self.dataset.model_version
        if '_' in dataset_model:
            dataset_model = 'E3SM-' + '-'.join(dataset.model_version.split('_'))
        if '_' in my_dataset_model:
            my_dataset_model = 'E3SM-' + '-'.join(self.dataset.model_version.split('_'))
        if dataset_model != my_dataset_model:
            return None

        dataset_ensemble = dataset.ensemble
        my_dataset_ensemble = self.dataset.ensemble
        if 'ens' in dataset_ensemble:
            dataset_ensemble = f"r{dataset.ensemble[3:]}i1p1f1"
        if 'ens' in my_dataset_ensemble:
            my_dataset_ensemble = f"r{self.dataset.ensemble[3:]}i1p1f1"
        if dataset_ensemble != my_dataset_ensemble:
            return None
        
        for req, ds in self._requires.items():
            if ds:
                continue
            req_attrs = req.split('-')

            if dataset.realm != req_attrs[0] and req_attrs[0] != '*':
                continue
            if dataset.grid != req_attrs[1] and req_attrs[1] != '*':
                continue
            if dataset.freq != req_attrs[2] and req_attrs[2] != '*':
                continue

            log_message('debug', f"found job requirement match")
            return req
        return None

    def meets_requirements(self):
        """
        Check if all the requirements for the job are met
        """
        for req in self._requires:
            if not self._requires.get(req):
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
