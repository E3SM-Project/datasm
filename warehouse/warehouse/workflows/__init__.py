import yaml
import importlib
import os
import inspect
from pprint import pformat
from pathlib import Path

NAME = 'Warehouse'


class Workflow(object):

    def __init__(self, parent=None, slurm_scripts='temp'):
        self.parent = parent
        self.transitions = {}
        self.children = {}
        self.slurm_scripts = slurm_scripts
        self.name = NAME.upper()
        self.jobs = self.load_jobs()
    
    def load_jobs(self):
        """
        get the path to the jobs directory which should be a sibling
        of the warehouse.py file
        """
        modules = {}
        jobs_path = Path(Path(inspect.getfile(self.__class__)).parent.absolute(), 'jobs')
        for file in jobs_path.glob('*'):
            if file.name == '__init__.py' or file.is_dir():
                continue
            module_string = f'warehouse.workflows.jobs.{file.stem}'
            module = importlib.import_module(module_string)
            job_class = getattr(module, module.NAME)
            modules[module.NAME] = job_class
        return modules


    def get_status_prefix(self, prefix=""):
        """
        From any node in the workflow tree, get the correct status message prefix
        """
        if self.parent != None:
            prefix = f'{self.name}:{prefix}'
            return self.parent.get_status_prefix(prefix)
        else:
            return prefix

    def next_state(self, dataset, state, params, idx=0):
        """
        Parameters: 
            dataset (Dataset) : The dataset which is changing state
            status (string) : The state to move out from
            idx (int) : The recursive depth index
        Returns the name of the next state to transition to given the current state of the dataset
        """
        state_attrs = state.split(':')
        if len(state_attrs) < 3:
            target_state = state
        else:
            target_state = f"{state_attrs[-3]}:{state_attrs[-2]}"
        prefix = self.get_status_prefix()
        if target_state in self.transitions.keys():
            target_data_type = f'{dataset.realm}-{dataset.grid}-{dataset.freq}'
            transitions = self.transitions[target_state].get(target_data_type)
            if transitions is None:
                return [(f'{prefix}{x}:', self, params) for x in self.transitions[target_state]['default']]
            else:
                return [(f'{prefix}{x}:', self, params) for x in transitions]
            
        elif state_attrs[idx] == "WAREHOUSE":
            return self.next_state(dataset, state, params, idx + 1)

        elif state_attrs[idx] in self.children.keys():
            return self.children[state_attrs[idx]].next_state(dataset, state, params, idx + 1)

        else:
            raise ValueError(
                f"{target_state} is not present in the transition graph for {self.name}")

    def get_job(self, dataset, state, params, scripts_path, slurm_out_path, workflow, job_workers=8, **kwargs):

        state_attrs = state.split(':')
        job_name = state_attrs[-3]

        parent = f"{state_attrs[0]}:{state_attrs[1]}"
    
        job = self.jobs[job_name]
        job_instance = job(
            dataset, 
            state, 
            scripts_path, 
            slurm_out_path,
            params=params,
            slurm_opts=kwargs.get('slurm_opts', []), 
            parent=parent,
            job_workers=job_workers)
        job_instance.setup_requisites()
        return job_instance
        

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
            workflow_instance = workflow_class(
                parent=self, 
                slurm_scripts=self.slurm_scripts)
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
