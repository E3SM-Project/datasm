import yaml
import importlib
import os
from pathlib import Path


class Workflow(object):

    def __init__(self, parent=None):
        self.parent = parent
        self.transitions = self.load_transitions()
        self.children = self.load_children()

    def next_state(self):
        ...

    def load_transitions(self):
        transition_path = Path(Path(__file__).parents[0], 'transitions.yaml')
        with open(transition_path, 'r') as instream:
            return yaml.SafeLoader(instream)
    
    def load_children(self):
        my_path = Path(__file__).parent.absolute()
        workflows = {}
        for d in os.scandir(my_path):
            if not d.is_dir():
                continue
            
            module_path = Path(my_path, d.name, '__init__.py')
            if not module_path.exists():
                raise ValueError(f"{module_path} doesnt exist, doesnt look like this is a well formatted workflow")
            
            workflows_string = f"warehouse{os.sep}workflows"
            idx = str(my_path.resolve()).find(workflows_string)
            if self.parent is None:
                module_name = f'warehouse.workflows.{d.name}'
            else:
                module_name = f'warehouse.workflows.{my_path[idx+len(workflows_string)].replace(os.sep, ".")}'

            import ipdb; ipdb.set_trace()
            module = importlib.import_module(module_name)
            workflow_class = getattr(module, module.NAME)
            workflow_instance = workflow_class(self)
            workflows[module.NAME] = module(self)
        return workflows



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
