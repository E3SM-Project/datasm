from pathlib import Path
from tempfile import NamedTemporaryFile


class WorkflowJob(object):

    def __init__(self, dataset, state, scripts_path, slurm_opts=[], **kwargs):
        super().__init__()
        self.name = "WorkflowJobBase"
        self._dataset = dataset
        self._starting_state = state
        self._scripts_path = scripts_path
        self._cmd = None
        self._requires = {}

    def __call__(self, slurm):
        if not self.meets_requirements():
            return None

        working_dir = self.dataset.working_dir
        if self.dataset.is_locked(working_dir):
            return None
        else:
            self.dataset.lock(working_dir)

        outname = f'{self.name}-{self._dataset.realm}-{self._dataset.grid}-{self._dataset.freq}.out'
        self._slurm_opts = slurm_opts.extend(
            [('-o', f'{Path(self._scripts_path, outname).resolve()}')])

        tmp = NamedTemporaryFile(dir=self._scripts_path, delete=False)
        slurm_run_script = Path(self._scripts_path, tmp)
        slurm.render_script(self.cmd, self._scripts_path, self._slurm_opts)
        return slurm.sbatch(self._scripts_path)

    def setup_requisites(self, input_datasets=None):
        """
        Checks that the self.dataset matches the jobs requirements, as well
        as an optional list of additional datasets
        """
        datasets = [self.dataset]
        if input_datasets:
            datasets.extend(input_datasets)

        for dataset in datasets:
            for req in self._requires:
                req_attrs = req.split('-')
                if dataset.realm in ['*', req_attrs[0]] \
                        and dataset.grid in ['*', req_attrs[1]] \
                        and dataset.freq in ['*', req_attrs[2]]:
                    self._requires[req] = self.dataset

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
