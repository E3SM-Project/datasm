import yaml
import importlib
import os
import inspect
from pprint import pformat
from pathlib import Path

NAME = 'Warehouse'

class Workflow(object):

    def __init__(self, parent=None):
        self.parent = parent
        self.transitions = {}
        self.children = {}
        self.name = NAME.upper()
    
    def get_status_prefix(self, prefix=""):
        if self.parent != None:
            prefix += f'{self.name}:{prefix}'
            return self.parent.get_status_prefix(prefix)
        else:
            return prefix

    def next_state(self, dataset, status):
        # Returns the name of the next state to transition to given the current state of the dataset
        status_attrs = status.split(':')
        if status_attrs[0] in self.children.keys():
            return self.children[status_attrs[0]].next_state(dataset, status)
        
        prefix = self.get_status_prefix()
        target_state = ":".join(status_attrs[-3:-1])
        if target_state in self.transitions.keys():
            target_data_type = f'{dataset.realm}-{dataset.grid}-{dataset.freq}'
            transitions = self.transitions[target_state].get(target_data_type)
            if not transitions:
                raise ValueError(f"{target_data_type} is not a transition from {self.transitions[target_state]}")
            transitions = [f'{prefix}{t}' for t in transitions]
            return transitions
        else:
            raise ValueError(f"{target_state} is not present in the transition graph for {NAME}")
        

        
    
    def get_job(self, state):
        # Returns a job instance for the given state name
        ...


    def load_transitions(self):
        transition_path = Path(Path(inspect.getfile(
            self.__class__)).parents[0], 'transitions.yaml')
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
                raise ValueError(
                    f"{module_path} doesnt exist, doesnt look like this is a well formatted workflow")

            workflows_string = f"warehouse{os.sep}workflows"
            idx = str(my_path.resolve()).find(workflows_string)
            if self.parent is None:
                module_name = f'warehouse.workflows.{d.name}'
            else:
                module_name = f'warehouse.workflows.{str(my_path)[idx+len(workflows_string) + 1:].replace(os.sep, ".")}.{d.name}'

            print(f"loading workflow module {module_name}")

            module = importlib.import_module(module_name)
            workflow_class = getattr(module, module.NAME)
            workflow_instance = workflow_class(parent=self)
            workflow_instance.load_children()
            workflow_instance.load_transitions()
            workflows[module.NAME.upper()] = workflow_instance
        self.children = workflows

    def toString(self):
        info = {}
        if self.parent == None:
            for name, instance in self.children.items():
                info[name] = instance.toString()
            return pformat(info, indent=4)
        else:
            if self.children:
                for name, instance in self.children.items():
                    info[name] = instance.toString()
                return info
            else:
                return self.transitions


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
