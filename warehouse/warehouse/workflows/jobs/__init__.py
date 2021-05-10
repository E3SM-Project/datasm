from pathlib import Path
from tempfile import NamedTemporaryFile
from termcolor import cprint


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
        self._job_workers = kwargs.get('job_workers', 8)
        self._job_id = None
        self._spec_path = kwargs.get('spec_path')
        self._config = kwargs.get('config') 
        self.debug = kwargs.get('debug')
    
    def resolve_cmd(self):
        return

    def __str__(self):
        return f"{self.parent}:{self.name}:{self.dataset.dataset_id}"

    def print_debug(self, msg):
        if self.debug:
            cprint(msg, 'yellow')

    def __call__(self, slurm):
        if not self.meets_requirements():
            print(f"Job does not meet requirements! {self.requires}")
            return None
        self.print_debug(f"Starting job: {str(self)} with reqs {[x.dataset_id for x in self.requires.values()]}")

        self.resolve_cmd()

        working_dir = self.dataset.latest_warehouse_dir
        if self.dataset.is_locked(working_dir):
            cprint(f"Cant start job working dir is locked: {working_dir}", "red")
            return None
        else:
            self.dataset.lock(working_dir)

        self._outname = f'{self._dataset.experiment}-{self.name}-{self._dataset.realm}-{self._dataset.grid}-{self._dataset.freq}-{self._dataset.ensemble}.out'
        output_option = (
            '-o', f'{Path(self._slurm_out, self._outname).resolve()}')

        self._slurm_opts.extend(
            [output_option, ('-N', 1), ('-c', self._job_workers)])

        script_name = f'{self._dataset.experiment}-{self.name}-{self._dataset.realm}-{self._dataset.grid}-{self._dataset.freq}-{self._dataset.ensemble}.sh'
        script_path = Path(self._slurm_out, script_name)
        script_path.touch()

        message_file = NamedTemporaryFile(dir=self._slurm_out, delete=False)
        Path(message_file.name).touch()
        self._cmd = f"export message_file={message_file.name}\n" + self._cmd

        self.add_cmd_suffix(working_dir)
        slurm.render_script(self.cmd, str(script_path), self._slurm_opts)
        self._job_id = slurm.sbatch(str(script_path))
        self.dataset.status = (f"{self._parent}:{self.name}:Engaged:", 
                               {"slurm_id": self.job_id})
        return self._job_id

    def add_cmd_suffix(self, working_dir):
        suffix = f"""
if [ $? -ne 0 ]
then 
    echo STAT:`date +%Y%m%d_%H%M%S`:WAREHOUSE:{self.parent}:{self.name}:Fail:`cat $message_file` >> {self.dataset.status_path}
else
    echo STAT:`date +%Y%m%d_%H%M%S`:WAREHOUSE:{self.parent}:{self.name}:Pass:`cat $message_file` >> {self.dataset.status_path}
fi
rm $message_file
"""
        self._cmd = self._cmd + suffix

    def setup_requisites(self, input_datasets=None, spec=None):
        """
        Checks that the self.dataset matches the jobs requirements, as well
        as an optional list of additional datasets
        """
        datasets = [self.dataset]
        if input_datasets:
            if not isinstance(input_datasets, list):
                input_datasets = [input_datasets]
            datasets.extend(input_datasets)

        for dataset in datasets:
            # cprint(f"checking if {dataset.dataset_id} is a good match for {self.name}")
            if (req := self.matches_requirement(dataset, spec)) is not None:
                # cprint(f'Found matching dataset! {dataset.dataset_id} for {self.dataset.dataset_id}', 'green')
                self._requires[req] = dataset
            # else:
                # cprint(f'{dataset.dataset_id} does not match for {self.dataset.dataset_id}', 'red')

    def matches_requirement(self, dataset, spec=None):
        """
        Checks that the self.dataset matches the jobs requirements, as well
        as an optional list of additional datasets
        """
        
        # cprint(f'checking {dataset.experiment} == {self.dataset.experiment}', "yellow")
        if dataset.experiment != self.dataset.experiment:
            # cprint(f'no match', "red")
            return None
        # cprint(f'match', "green")
        
        dataset_model = dataset.model_version
        my_dataset_model = self.dataset.model_version
        if '_' in dataset_model:
            dataset_model = 'E3SM-' + '-'.join(dataset.model_version.split('_'))
        if '_' in my_dataset_model:
            my_dataset_model = 'E3SM-' + '-'.join(self.dataset.model_version.split('_'))
        # cprint(f'checking {dataset_model} == {my_dataset_model}', "yellow")
        if dataset_model != my_dataset_model:
            # cprint(f'no match', "red")
            return None
        # cprint(f'match', "green")


        dataset_ensemble = dataset.ensemble
        my_dataset_ensemble = self.dataset.ensemble
        if 'ens' in dataset_ensemble:
            dataset_ensemble = f"r{dataset.ensemble[3:]}i1p1f1"
        if 'ens' in my_dataset_ensemble:
            my_dataset_ensemble = f"r{self.dataset.ensemble[3:]}i1p1f1"
        # cprint(f'checking {dataset_ensemble} == {my_dataset_ensemble}', "yellow")
        if dataset_ensemble != my_dataset_ensemble:
            # cprint(f'no match', "red")
            return None
        # cprint(f'match', "green")

        # if dataset.experiment != self.dataset.experiment \
        #         or dataset.model_version != self.dataset.model_version \
        #         or dataset.ensemble != self.dataset.ensemble:
        #     return None
        # import ipdb
        # ipdb.set_trace()
        dataset_facets = dataset.dataset_id.split('.')
        if spec is not None and 'CMIP' in self.dataset.dataset_id and dataset_facets[0] == 'E3SM':
            e3sm_cmip_case = spec['projects']['E3SM'][dataset_facets[1]][dataset_facets[2]].get('cmip_case')
            if not e3sm_cmip_case:
                return None
            
            my_dataset_facets = self.dataset.dataset_id.split('.')
            my_case_attrs = '.'.join(my_dataset_facets[:4])
            if not my_case_attrs == e3sm_cmip_case:
                return None
            
        for req, ds in self._requires.items():
            if ds:
                continue
            req_attrs = req.split('-')

            # cprint(f'checking {dataset.realm} == {req_attrs[0]}', "yellow")
            if dataset.realm != req_attrs[0] and req_attrs[0] != '*':
                # cprint(f'no match', "red")
                return None
            # cprint(f'match', "green")
            # cprint(f'checking {dataset.grid} == {req_attrs[1]}', "yellow")
            if dataset.grid != req_attrs[1] and req_attrs[1] != '*':
                # cprint(f'no match', "red")
                return None
            # cprint(f'match', "green")
            # cprint(f'checking {dataset.freq} == {req_attrs[2]}', "yellow")
            if dataset.freq != req_attrs[2] and req_attrs[2] != '*':
                # cprint(f'no match', "red")
                return None
            # cprint(f'match', "green")
            return req
            # if (dataset.realm == req_attrs[0] or req_attrs[0] == '*') \
            #         and (dataset.grid == req_attrs[1] or req_attrs[1] == '*') \
            #         and (dataset.freq == req_attrs[2] or req_attrs[2] == '*'):
            #     return req
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
