import yaml
from pathlib import Path

class Workflow(object):

    def __init__(self):
        self.transitions = loadTransitions()
    
    def nextState(self):
        ...

    def loadTransitions(self):
        transition_path = Path(Path(__file__).parents[0], 'transitions.yaml')
        with open(transition_path, 'r') as instream:
            return yaml.SafeLoader(instream)

class WorkflowJob(object):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dataset = kwargs.get('dataset')
        self._cmd = None

    def sbatch_submit(self):
        ...
    
    @property
    def dataset(self):
        return self._dataset
    
    @dataset.setter
    def dataset(self, new_ds):
        self._dataset = new_ds