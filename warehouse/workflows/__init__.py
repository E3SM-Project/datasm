import yaml
import importlib
import os
import inspect
from pathlib import Path


class Workflow(object):

    def __init__(self, parent=None):
        self.parent = parent
        self.transitions = {}
        self.children = {}

    def next_state(self):
        ...

    def load_transitions(self):
        transition_path = Path(Path(inspect.getfile(self.__class__)).parents[0], 'transitions.yaml')
        with open(transition_path, 'r') as instream:
            self.transitions = yaml.load(instream, Loader=yaml.SafeLoader)
    
    def load_children(self):
        my_path = Path(inspect.getfile(self.__class__)).parent.absolute()
        workflows = {}
        for d in os.scandir(my_path):
            if not d.is_dir() or d.name == "jobs" or d.name == "__pycache__":
                continue
            
            module_path = Path(my_path, d.name, '__init__.py')
            if not module_path.exists():
                raise ValueError(f"{module_path} doesnt exist, doesnt look like this is a well formatted workflow")
            
            workflows_string = f"warehouse{os.sep}workflows"
            idx = str(my_path.resolve()).find(workflows_string)
            if self.parent is None:
                module_name = f'warehouse.workflows.{d.name}'
            else:
                module_name = f'warehouse.workflows.{str(my_path)[idx+len(workflows_string) + 1:].replace(os.sep, ".")}'

            print(f"loading workflow module {module_name}")
            module = importlib.import_module(module_name)
            workflow_class = getattr(module, module.NAME)
            workflow_instance = workflow_class(parent=self)
            workflow_instance.load_children()
            workflow_instance.load_transitions()
            workflows[module.NAME] = workflow_instance
        self.children = workflows
    
    def get_status_prefix(self, prefix=""):
        if self.parent != None:
            self.prefix += ":" + self.NAME
            return self.parent.get_status_prefix(prefix)
        else:
            return self.NAME + prefix



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
