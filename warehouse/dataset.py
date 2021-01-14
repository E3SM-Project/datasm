import os
import re
import json
from enum import Enum

from pathlib import Path
from pprint import pprint
from datetime import datetime
from warehouse.util import load_file_lines, sproket_with_id


class DatasetStatus(Enum):
    UNITITIALIZED = 1
    INITIALIZED = 2
    RUNNING = 4
    FAILED = 5
    SUCCESS = 6
    IN_WAREHOUSE = 8
    IN_PUBLICATION = 9
    NOT_PUBLISHED = 10
    NOT_IN_PUBLICATION = 11
    NOT_IN_WAREHOUSE = 12
    PARTIAL_PUBLISHED = 13


class DatasetStatusMessage(Enum):
    PUBLICATION_READY: "DATASET:PUBLICATION:Ready"
    WAREHOUSE_READY: "DATASET:WAREHOUSE:Ready"


non_binding_status = ['Blocked', 'Unblocked', 'Approved', 'Unapproved']


SEASONS = [{
    'name': 'ANN',
    'start': '01',
    'end': '12'
}, {
    'name': 'DJF',
    'start': '01',
    'end': '12'
}, {
    'name': 'MAM',
    'start': '03',
    'end': '05'
}, {
    'name': 'JJA',
    'start': '06',
    'end': '08'
}, {
    'name': 'SON',
    'start': '09',
    'end': '11'
}]


class Dataset(object):
    def __init__(self, dataset_id, pub_base=None, warehouse_base=None, archive_base=None, start_year=None, end_year=None, datavars=None, path='', versions={}, stat=None, comm=None, *args, **kwargs):
        super().__init__()
        self.dataset_id = dataset_id
        self.status = DatasetStatus.UNITITIALIZED
        self.status_path = None
        self.start_year = start_year
        self.end_year = end_year
        self.datavars = datavars
        self.missing = None
        self.publication_path = Path(path)
        self.pub_base = pub_base
        self.warehouse_path = Path(path)
        self.warehouse_base = warehouse_base
        self.archive_path = Path(path)
        self.archive_base = archive_base
        self.sproket = kwargs.get('sproket')

        self.stat = stat if stat else {}
        self.comm = comm if comm else {}
        self.versions = versions

        facets = self.dataset_id.split('.')
        if facets[0] == 'CMIP':
            self.project = 'CMIP'
            self.data_type = 'time-series'
            self.activity = facets[2]
            self.model_version = facets[3]
            self.experiment = facets[4]
            self.ensemble = facets[5]
            self.table = facets[6]
            self.resolution = None
            self.realm = None
            self.freq = None  # the frequency and realm are part of the CMIP table
            self.grid = 'gr'
        else:
            self.project = 'E3SM'
            self.model_version = facets[1]
            self.experiment = facets[2]
            self.resolution = facets[3]
            self.realm = facets[4]
            self.grid = facets[5]
            self.data_type = facets[6]
            self.freq = facets[7]
            self.ensemble = facets[8]
            self.activity = None
            self.table = None

    def datatype_from_id(self):
        if 'CMIP' in self.dataset_id:
            return 'CMIP'

        facets = self.dataset_id.split('.')
        # should be component.grid.type.freq, e.g. "atmos.180x360.time-series.mon"
        return '.'.join(facets[4:7])

    def get_latest_status(self):
        latest = '0'
        latest_val = None
        for major in self.stat.keys():
            for minor in self.stat[major].keys():
                for item in self.stat[major][minor]:
                    if item[0] >= latest and item[1] not in non_binding_status:
                        latest = item[0]
                        latest_val = f'{minor}:{item[1]}'
        return latest, latest_val

    def check_dataset_is_complete(self, files):

        # filter out files from old versions
        nfiles = []
        for file in files:
            file_path, name = os.path.split(file)
            file_attrs = file_path.split('/')
            version = file_attrs[-1]
            nfiles.append((version, file))

        latest_version = sorted(nfiles)[-1][0]
        files = [x for version, x in nfiles if version == latest_version]
        self.versions = {latest_version: len(files)}

        if not self.start_year or not self.end_year:
            if 'CMIP' in self.dataset_id:
                self.start_year, self.end_year = self.infer_start_end_cmip(
                    files)
            else:
                if self.data_type == 'time-series':
                    self.start_year, self.end_year = self.get_ts_start_end(
                        files[0])
                elif self.data_type == 'climo':
                    self.start_year, self.end_year = self.infer_start_end_climo(
                        files)
                else:
                    self.start_year, self.end_year = self.infer_start_end_e3sm(
                        files)

        # if not self.start_year or not self.end_year:
        #     import ipdb; ipdb.set_trace()
        #     this is probably a fx dataset

        if 'CMIP' in self.dataset_id:
            missing = self.check_spans(files)
        else:
            if 'model-output.mon' in self.dataset_id:
                missing = self.check_monthly(files)
            elif 'climo' in self.dataset_id:
                missing = self.check_climos(files)
            elif 'time-series' in self.dataset_id:
                missing = self.check_time_series(files)
            elif 'fixed' in self.dataset_id:
                # missing, extra = check_fixed(files, dataset_id, spec)
                # TODO: implement this
                missing = []
            else:
                missing = self.check_submonthly(files)

        if missing:
            self.missing = missing
            return False
        else:
            return True

    def get_esgf_status(self, sproket='sproket'):
        """
        Check ESGF to see of the dataset has already been published,
        if it exists check that the dataset is complete"""
        _, files = sproket_with_id(self.dataset_id, sproket_path=self.sproket)
        if not files:
            return DatasetStatus.UNITITIALIZED

        is_complete = self.check_dataset_is_complete(files)

        if is_complete:
            return DatasetStatus.SUCCESS
        else:
            return DatasetStatus.PARTIAL_PUBLISHED

    def get_status_from_pub_dir(self):
        if self.project == 'CMIP':
            pubpath = Path(
                self.pub_base,
                self.project,
                self.activity,
                self.model_version,
                self.experiment,
                self.ensemble,
                self.table,
                self.grid)
        else:
            pubpath = Path(
                self.pub_base,
                self.project,
                self.model_version,
                self.experiment,
                self.resolution,
                self.realm,
                self.grid,
                self.data_type,
                self.freq,
                self.ensemble)

        self.publication_path = pubpath
        if not self.publication_path.exists():
            return DatasetStatus.NOT_IN_PUBLICATION

        self.versions = {
            x: len([i for i in Path(self.path, x).glob('*')])
            for x in os.listdir(self.path) if x[0] == 'v'}

        statfile = Path(self.publication_path, '.status')
        if statfile.exists():
            self.status_path = statfile
            self.load_dataset_status_file(statfile)

        version_names = list(self.versions.keys())
        version_dir = Path(self.publication_path, version_names[-1])
        files = [x.resolve() for x in version_dir.glob('*')]
        if not files:
            return DatasetStatus.NOT_IN_PUBLICATION

        is_complete = self.check_dataset_is_complete(files)
        if is_complete:
            # we only set the status file if the publication is complete
            # otherwise the "official" location should be the warehouse
            self.status_path = statfile
            self.update_status(DatasetStatusMessage.PUBLICATION_READY)
            return DatasetStatus.IN_PUBLICATION
        else:
            return DatasetStatus.NOT_IN_PUBLICATION

    def update_status(self, status):
        if not self.status_path or not self.status_path.exists():
            raise ValueError(
                f"Invalid status path {self.status_path} for dataset {self.dataset_id}")
        with open(self.status_path, 'a') as outstream:
            msg = f'STAT:{datetime.now().strftime("%Y%m%d_%H%M%S")}:{status}'

    def get_status_from_warehouse(self):
        if self.project == 'CMIP':
            warepath = Path(
                self.warehouse_base,
                self.project,
                self.activity,
                self.model_version,
                self.experiment,
                self.ensemble,
                self.table,
                self.grid)
        else:
            warepath = Path(
                self.warehouse_base,
                self.project,
                self.model_version,
                self.experiment,
                self.resolution,
                self.realm,
                self.grid,
                self.data_type,
                self.freq,
                self.ensemble)
        self.warehouse_path = warepath
        if not self.warehouse_path.exists():
            return DatasetStatus.NOT_IN_WAREHOUSE

        self.versions = {
            x: len([i for i in Path(self.warehouse_path, x).glob('*')])
            for x in os.listdir(self.warehouse_path) if x[0] == 'v'}

        statfile = Path(self.warehouse_path, '.status')
        if statfile.exists():
            self.status_path = statfile
            self.load_dataset_status_file(statfile)
            _, status = self.get_latest_status()
            return status

        version_names = list(self.versions.keys())
        version_dir = Path(self.publication_path, version_names[-1])
        files = [x.resolve() for x in version_dir.glob('*')]
        if not files:
            return DatasetStatus.NOT_IN_WAREHOUSE

    def get_status_from_archive(self):
        ...

    def find_status(self, sproket='sproket'):
        """
        Lookup the datasets status in ESGF, or on the filesystem
        """
        # if the dataset is UNITITIALIZED, then we need to build up the status from scratch
        if self.status == DatasetStatus.UNITITIALIZED:
            # returns either NOT_PUBLISHED or SUCCESS or PARTIAL_PUBLISHED or UNITITIALIZED
            self.status = self.get_esgf_status(sproket)

        # TODO: figure out how to update and finish a dataset thats been published
        # but is missing some of its files
        # if self.status == DatasetStatus.PARTIAL_PUBLISHED:
        #    ...

        if self.status in [DatasetStatus.NOT_PUBLISHED, DatasetStatus.UNITITIALIZED]:
            # returns IN_PUBLICATION or NOT_IN_PUBLICATION
            self.status = self.get_status_from_pub_dir()

        if self.status in [DatasetStatus.NOT_IN_PUBLICATION, DatasetStatus.UNITITIALIZED]:
            # returns IN_WAREHOUSE or NOT_IN_WAREHOUSE
            self.status = self.get_status_from_warehouse()

        if self.status in [DatasetStatus.NOT_IN_WAREHOUSE, DatasetStatus.UNITITIALIZED] and self.data_type not in ['time-series', 'climo']:
            # returns IN_ARCHIVE OR NOT_IN_ARCHIVE
            self.status = self.get_status_from_archive()

        return self.dataset_id, self.status



    def initialize_status_file(self):
        ...

    def check_submonthly(self, files):
        missing = list()
        files = sorted(files)

        first = files[0]
        pattern = re.compile(r'\d{4}-\d{2}.*nc')
        idx = pattern.search(first)
        if not idx:
            raise ValueError(f'Unexpected file format: {first}')

        prefix = first[:idx.start()]
        # TODO: Come up with a way of doing this check more
        # robustly. Its hard because the high-freq files arent consistant
        # from case to case, using different 'h' codes and different frequencies
        # for the time being, if there's at least one file per year it'll get marked as correct
        for year in range(self.start_year, self.end_year):
            found = None
            pattern = re.compile(f'{year:04d}' + r'-\d{2}.*.nc')
            for idx, file in enumerate(files):
                if pattern.search(file):
                    found = idx
                    break
            if found is not None:
                files.pop(idx)
            else:
                name = f'{prefix}{year:04d}'
                missing.append(name)

        return missing

    def check_time_series(self, files):

        missing = []
        files = [x.split('/')[-1] for x in sorted(files)]
        files_found = []

        if not self.datavars:
            raise ValueError(
                f"dataset {self.dataset_id} is trying to validate time-series files, but has no datavars")

        for var in self.datavars:

            # depending on the mapping file used to regrid the time-series
            # they may have different names, so we start by finding
            # all the files for each variable
            v_files = list()
            for x in files:
                idx = -36 if 'cmip6_180x360_aave' in x else -17
                if var in x and x[:idx] == var:
                    v_files.append(x)

            if not v_files:
                missing.append(
                    f'{dataset_id}-{var}-{self.start_year:04d}-{self.end_year:04d}')
                continue

            v_files = sorted(v_files)
            v_start, v_end = self.get_ts_start_end(v_files[0])
            if self.start_year != v_start:
                missing.append(
                    f'{self.dataset_id}-{var}-{self.start_year:04d}-{v_start:04d}')

            prev_end = self.start_year
            for file in v_files:
                file_start, file_end = self.get_ts_start_end(file)
                if file_start == self.start_year:
                    prev_end = file_end
                    continue
                if file_start == prev_end + 1:
                    prev_end = file_end
                else:
                    missing.append(
                        f"{self.dataset_id}-{prev_end:04d}-{file_start:04d}")

            file_start, file_end = self.get_ts_start_end(files[-1])
            if file_end != self.end_year:
                missing.append(
                    f"{self.dataset_id}-{file_start:04d}-{self.end_year:04d}")

        return missing

    def check_monthly(self, files):
        """
        Given a list of monthly files, find any that are missing
        """
        missing = []
        files = sorted(files)

        pattern = r'\d{4}-\d{2}.*nc'
        idx = re.search(pattern=pattern, string=files[0])
        if not idx:
            raise ValueError(f'Unexpected file format: {files[0]}')

        prefix = files[0][:idx.start()]
        suffix = files[0][idx.start() + 7:]

        for year in range(self.start_year, self.end_year + 1):
            for month in range(1, 13):
                name = f'{prefix}{year:04d}-{month:02d}{suffix}'
                if name not in files:
                    missing.append(name)

        return missing

    def check_climos(self, files):
        """
        Given a list of climo files, find any that are missing
        """
        missing = []

        pattern = r'_\d{6}_\d{6}_climo.nc'
        files = sorted(files)
        idx = re.search(pattern=pattern, string=files[0])
        if not idx:
            raise ValueError(f'Unexpected file format: {files[0]}')
        prefix = files[0][:idx.start() - 2]

        for month in range(1, 13):
            name = f'{prefix}{month:02d}_{self.start_year:04d}{month:02d}_{self.end_year:04d}{month:02d}_climo.nc'
            if name not in files:
                missing.append(name)

        for season in SEASONS:
            name = f'{prefix}{season["name"]}_{self.start_year:04d}{season["start"]}_{self.end_year:04d}{season["end"]}_climo.nc'
            if name not in files:
                missing.append(name)

        return missing

    @staticmethod
    def get_file_start_end(filename):
        if 'clim' in filename:
            return int(filename[-21:-17]), int(filename[-14: -10])
        else:
            return int(filename[-16:-12]), int(filename[-9: -5])

    @staticmethod
    def get_ts_start_end(filename):
        p = re.compile(r'_\d{6}_\d{6}.*nc')
        idx = p.search(filename)
        if not idx:
            raise ValueError(f'Unexpected file format: {filename}')
        start = int(filename[idx.start() + 1: idx.start() + 5])
        end = int(filename[idx.start() + 8: idx.start() + 12])
        return start, end

    def check_spans(self, files):
        """
        Given a list of CMIP files, find of all the files that should be there are
        """
        missing = []
        files = sorted(files)

        file_start, file_end = get_file_start_end(files[0])

        if file_start != self.start_year:
            missing.append(
                f"{dataset_id}-{self.start_year:04d}-{file_end:04d}")

        prev_end = self.start_year
        for file in files:
            file_start, file_end = self.get_file_start_end(file)
            if file_start == self.start_year:
                prev_end = file_end
                continue
            if file_start == prev_end + 1:
                prev_end = file_end
            else:
                missing.append(f"{dataset_id}-{prev_end:04d}-{file_start:04d}")

        file_start, file_end = self.get_file_start_end(files[-1])
        if file_end != self.end_year:
            missing.append(
                f"{dataset_id}-{file_start:04d}-{self.end_year:04d}")
        return missing

    def infer_start_end_cmip(self, files):
        """
        From a list of files with the given naming convention
        return the start year of the first file and the end year of the
        last file

        A typical CMIP6 file will have a name like:
        pbo_Omon_E3SM-1-1-ECA_hist-bgc_r1i1p1f1_gr_185001-185412.nc' 
        """
        files = sorted(files)
        first, last = files[0], files[-1]
        p = r'\d{6}-\d{6}'
        idx = re.search(pattern=p, string=first)
        if not idx:
            return None, None
        start = int(first[idx.start(): idx.start() + 4])
        idx = re.search(pattern=p, string=last)
        end = int(last[idx.start() + 7: idx.start() + 11])
        return start, end

    def infer_start_end_e3sm(self, files):
        """
        From a list of files with the given naming convention
        return the start year of the first file and the end year of the
        last file
        """
        f = sorted(files)
        p = r'\.\d{4}-\d{2}'
        idx = re.search(pattern=p, string=f[0])
        if not idx:
            return None, None
        start = int(f[0][idx.start() + 1: idx.start() + 5])
        idx = re.search(pattern=p, string=f[-1])
        end = int(f[-1][idx.start() + 1: idx.start() + 5])
        return start, end

    @staticmethod
    def infer_start_end_climo(files):
        f = sorted(files)
        p = r'_\d{6}_\d{6}_'
        idx = re.search(pattern=p, string=f[0])
        start = int(f[0][idx.start() + 1: idx.start() + 5])

        idx = re.search(pattern=p, string=f[-1])
        end = int(f[-1][idx.start() + 8: idx.start() + 12])

        return start, end

    def is_blocked(self):
        ...

    def __str__(self):
        return f"""id: {self.dataset_id},
path: {self.path},
version: {', '.join(self.versions.keys())},
stat: {json.dumps(self.stat, indent=4)},
comm: {self.comm}"""

    def load_dataset_status_file(self, path):
        """
        read status file, convert lines "STAT:ts:PROCESS:status1:status2:..."
        into dictionary, key = STAT, rows are tuples (ts,'PROCESS:status1:status2:...')
        and for comments, key = COMM, rows are comment lines
        """
        if not path.exists():
            return dict()

        statbody = load_file_lines(path.resolve())
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
                self.stat[major][minor].append(
                    (timestamp, ':'.join(line_info[4:])))
            else:
                self.comm.append(line)
        return
