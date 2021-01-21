from pathlib import Path
from tempfile import NamedTemporaryFile


class WorkflowJob(object):

    def __init__(self, dataset, state, scripts_path, slurm_out_path, slurm_opts=[], **kwargs):
        super().__init__()
        self.name = "WorkflowJobBase"
        self._dataset = dataset
        self._starting_state = state
        self._scripts_path = scripts_path
        self._slurm_out = slurm_out_path
        self._slurm_opts = slurm_opts
        self._cmd = None
        self._outname = None
        self._requires = {}
        self._parent = kwargs.get('parent')

    def __call__(self, slurm):
        if not self.meets_requirements():
            return None

        working_dir = self.dataset.working_dir
        if self.dataset.is_locked(working_dir):
            return None
        else:
            self.dataset.lock(working_dir)

        self._outname = f'{self._dataset.experiment}-{self.name}-{self._dataset.realm}-{self._dataset.grid}-{self._dataset.freq}.out'
        output_option = ('-o', f'{Path(self._slurm_out, self._outname).resolve()}')
        self._slurm_opts.extend([output_option])

        tmp = NamedTemporaryFile(dir=self._scripts_path, delete=False)

        self.add_cmd_suffix()
        slurm.render_script(self.cmd, tmp.name, self._slurm_opts)
        self.dataset.update_status(f"{self._parent}:{self.name}:Engaged")
        return slurm.sbatch(tmp.name)

    
    def add_cmd_suffix(self):
        suffix = f"""
if [ $? -ne 0 ]
then 
    echo STAT:`date +%Y%m%d_%H%M%S`:{self.parent}:{self.name}:Fail >> {self.dataset.status_path}
else
    echo STAT:`date +%Y%m%d_%H%M%S`:{self.parent}:{self.name}:Pass >> {self.dataset.status_path}
fi
"""
        self._cmd = self._cmd + suffix

    def setup_requisites(self, input_datasets=None):
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
            if (req:=self.matches_requirement(dataset)) is not None:
                self._requires[req] = dataset
    
    def matches_requirement(self, dataset):
        """
        Checks that the self.dataset matches the jobs requirements, as well
        as an optional list of additional datasets
        """

        if dataset.experiment != self.dataset.experiment \
        or dataset.model_version != self.dataset.model_version \
        or dataset.ensemble != self.dataset.ensemble:
            return None

        for req, ds in self._requires.items():
            if ds:
                continue
            req_attrs = req.split('-')
            if (dataset.realm == req_attrs[0] or req_attrs[0] == '*') \
            and (dataset.grid == req_attrs[1] or req_attrs[1] == '*') \
            and (dataset.freq == req_attrs[2] or req_attrs[2] == '*'):
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
        latest_path = self._dataset.working_dir
        # assuming the path ends in something like "v0" or "v0.1"
        version = latest_path.split('v')[-1]
        if '.' in version:
            version_number = int(version.split('.')[-1])
        else:
            version_number = int(version)
        new_version = version_number + 1
        return Path(Path(self._dataset.working_dir).parents[0], f"v0.{new_version}")

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
