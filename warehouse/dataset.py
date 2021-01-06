import os
import json
from pathlib import Path
from pprint import pprint
from warehouse.util import load_file_lines


class Dataset(object):
    def __init__(self, path='', versions=None, stat=None, comm=None, data_type=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = Path(path)
        self.stat = stat if stat else {}
        self.comm = comm if comm else []
        self.data_type = data_type if data_type else None
        if versions:
            self.versions = versions
        else:
            # if the vdir isnt given, find the highest numbered version
            self.versions = {x: len(os.listdir(Path(self.path, x).resolve())) for x in os.listdir(self.path) if x[0] == 'v'}
        if self.path.exists():
            self.load_dataset_status_file()
    
    def get_latest(self):
        latest = '0'
        latest_val = None
        for major in self.stat.keys():
            for minor in self.stat[major].keys():
                for item in self.stat[major][minor]:
                    if item[0] > latest:
                        latest = item[0]
                        latest_val = f'{minor}:{item[1]}'
        return latest, latest_val

    def is_blocked(self):
        ...
    
    def __str__(self):
        return f"""path: {self.path},
version: {', '.join(self.versions.keys())},
stat: {json.dumps(self.stat, indent=4)},
comm: {self.comm}"""

    def load_dataset_status_file(self):
        """
        read status file, convert lines "STAT:ts:PROCESS:status1:status2:..."
        into dictionary, key = STAT, rows are tuples (ts,'PROCESS:status1:status2:...')
        and for comments, key = COMM, rows are comment lines
        """
        statfile = Path(self.path, '.status')
        if not statfile.exists():
            return dict()

        statbody = load_file_lines(statfile.resolve())
        for line in statbody:
            line_info = [x for x in line.split(':') if x]
            # forge tuple (timestamp,residual_string), add to STAT list
            if line_info[0] == 'STAT':
                timestamp = line_info[1]
                major = line_info[2]
                minor = line_info[3]
                status = line_info[4]
                if len(line_info) > 5:
                    args = line_info[5:]
                
                if major not in self.stat:
                    self.stat[major] = {}
                if minor not in self.stat[major]:
                    self.stat[major][minor] = []
                self.stat[major][minor].append((timestamp, ':'.join(line_info[4:])))
            else:
                self.comm.append(line)
        return