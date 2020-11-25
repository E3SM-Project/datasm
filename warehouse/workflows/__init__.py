import yaml
from pathlib import Path

class Workflow(object):

    def __init__(self):
        self.transitions = loadTransitions()
    
    def nextState(self):
        ...

    def loadTransition(self):
        transition_path = Path(Path(__file__).parents[0], 'transitions.yaml')
        with open(transition_path, 'r') as instream:
            return yaml.SafeLoader(instream)