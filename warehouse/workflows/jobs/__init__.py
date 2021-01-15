from warehouse.slurm import Slurm

class WorkflowJob(object):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dataset = kwargs.get('dataset')
        self._cmd = None

    @property
    def dataset(self):
        return self._dataset

    @dataset.setter
    def dataset(self, new_ds):
        self._dataset = new_ds
