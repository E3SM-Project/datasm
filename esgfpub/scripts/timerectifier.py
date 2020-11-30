import os
import sys
import re
import argparse
import xarray as xr
import numpy as np
import netCDF4
from tqdm import tqdm
from datetime import datetime
from shutil import copyfile
from concurrent.futures import ProcessPoolExecutor, as_completed


from collections.abc import MutableSet

class OrderedSet(MutableSet):

    def __init__(self, iterable=None):
        self.end = end = [] 
        end += [None, end, end]         # sentinel node for doubly linked list
        self.map = {}                   # key --> [key, prev, next]
        if iterable is not None:
            self |= iterable

    def __len__(self):
        return len(self.map)

    def __contains__(self, key):
        return key in self.map

    def add(self, key):
        if key not in self.map:
            end = self.end
            curr = end[1]
            curr[2] = end[1] = self.map[key] = [key, curr, end]

    def discard(self, key):
        if key in self.map:        
            key, prev, next = self.map.pop(key)
            prev[2] = next
            next[1] = prev

    def __iter__(self):
        end = self.end
        curr = end[2]
        while curr is not end:
            yield curr[0]
            curr = curr[2]

    def __reversed__(self):
        end = self.end
        curr = end[1]
        while curr is not end:
            yield curr[0]
            curr = curr[1]

    def pop(self, last=True):
        if not self:
            raise KeyError('set is empty')
        key = self.end[1][0] if last else self.end[2][0]
        self.discard(key)
        return key

    def __repr__(self):
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, list(self))

    def __eq__(self, other):
        if isinstance(other, OrderedSet):
            return len(self) == len(other) and list(self) == list(other)
        return set(self) == set(other)

def update_history(ds):
    '''Add or append history to attributes of a data set'''

    thiscommand = datetime.now().strftime("%a %b %d %H:%M:%S %Y") + ": " + \
        " ".join(sys.argv[:])
    if 'history' in ds.attrs:
        newhist = '\n'.join([thiscommand, ds.attrs['history']])
    else:
        newhist = thiscommand
    ds.attrs['history'] = newhist

def write_netcdf(ds, fileName, fillValues=netCDF4.default_fillvals, unlimited=None):
    '''Write an xarray Dataset with NetCDF4 fill values where needed'''
    encodingDict = {}
    variableNames = list(ds.data_vars.keys()) + list(ds.coords.keys())
    for variableName in variableNames:
        isNumeric = np.issubdtype(ds[variableName].dtype, np.number)
        if isNumeric:
            dtype = ds[variableName].dtype
            for fillType in fillValues:
                if dtype == np.dtype(fillType):
                    encodingDict[variableName] = \
                        {'_FillValue': fillValues[fillType]}
                    break
        else:
            encodingDict[variableName] = {'_FillValue': None}

    update_history(ds)

    if unlimited:
        ds.to_netcdf(fileName, encoding=encodingDict, unlimited_dims=unlimited)
    else:    
        ds.to_netcdf(fileName, encoding=encodingDict)

def check_indices(f1, f2):

    ds1 = xr.open_dataset(f1, decode_times=False)
    ds2 = xr.open_dataset(f2, decode_times=False)

    # the ending bound of the first file       
    last_first_bound = ds1['time_bnds'][-1].values[-1]
    # the first bound of the second file
    first_last_bound = ds2['time_bnds'][0].values[0]

    if last_first_bound != first_last_bound:
        _, n1 = os.path.split(f1)
        _, n2 = os.path.split(f2)
        msg = f"{n1}, {n2}"
        return msg
    return False

def deoverlap(segments, streams, inpath, outpath, copy=None):
    """
    First copy/link over all the files without overlaps from the correct file streams
    
    Then truncate the first file from the overlapping pairs
    """
    os.makedirs(outpath, exist_ok=True)
    index = 0
    
    pathpairs = []
    for file in os.listdir(inpath):
        should_continue = False
        for segment in segments:
            # if this file is the start of a new segment
            #  skip it and move to the next file stream
            if file == segment[0]:
                index += 1
                should_continue = True
                break
        if should_continue:
            continue

        if streams[index] in file:
            src = os.path.join(os.path.abspath(inpath), file)
            dst = os.path.join(outpath, file)
            pathpairs.append((src, dst))
    
    # move the files over in parallel
    if copy:
        desc = "Copying files into output directory"
        move = copyfile
    else:
        desc = "Linking files into output directory"
        move = os.symlink
    with ProcessPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(move, src, dst) for src, dst in pathpairs]
        for _ in tqdm(as_completed(futures), total=len(futures), desc=desc):
            pass

    for segment in segments:

        with xr.open_dataset(os.path.join(os.path.abspath(inpath), segment[1]), decode_times=False) as ds:
            end_index = ds['time_bnds'][0].values[0]
        
        new_ds = xr.Dataset()
        with xr.open_dataset(os.path.join(os.path.abspath(inpath), segment[0]), decode_times=False) as ds:
            target_index = 0
            for i in range(0, len(ds['time_bnds'])):
                if ds['time_bnds'][i].values[0] == end_index:
                    break
                target_index += 1

            new_ds.attrs = ds.attrs
            for variable in ds.data_vars:
                if 'time' not in ds[variable].coords:
                    new_ds[variable] = ds[variable]
                    new_ds[variable].attrs = ds[variable].attrs
                    continue
                new_ds[variable] = ds[variable].isel(time=slice(0, target_index))
                new_ds[variable].attrs = ds[variable].attrs
        
        outfile_path = os.path.join(outpath, f"{segment[0][:-3]}.trunc.nc")
        print(f"writing out {outfile_path}")
        write_netcdf(new_ds, outfile_path, unlimited=['time'])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help="The directory to check for time index issues, should only contain a single time-frequency from a single case")
    parser.add_argument('--output', default="output", required=False, help=f"output directory for rectified dataset, default is {os.environ['PWD']}/output")
    parser.add_argument('--copy', action="store_true", required=False, help="create a copy of the files in the output directory instead of symlinks")
    parser.add_argument('-j', '--jobs', default=8, type=int, help="the number of processes, default is 8")
    args = parser.parse_args()
    inpath = args.input
    outpath = args.output
    num_jobs = args.jobs

    # collect all the files and sort them by their date stamp
    names = [os.path.join(inpath, x) for x in os.listdir(inpath) if x.endswith('.nc')]
    pattern = r'\d{4}-\d{2}-\d{2}'
    files = []
    for name in names:
        start = re.search(pattern, name).start()
        files.append({
            'prefix': name[:start],
            'suffix': name[start:]
        })
    files = sorted(files, key=lambda i: i['suffix'])

    # push the case names into an ordered set
    streams = [x for x in OrderedSet([i['prefix'][len(inpath) + 1:] for i in files])]

    # reassemble the file names in their sorted order
    files = [x['prefix'] + x['suffix'] for x in files]


    # check all the ordered pairs of files (n, n+1), (n+1, n+2), etc in parallel
    pairs = zip(files[:-1], files[1:])
    futures = []
    overlap = []
    print(f'Starting overlap check with {num_jobs} workers')
    with ProcessPoolExecutor(max_workers=num_jobs) as pool:
        futures = [pool.submit(check_indices, pair[0], pair[1]) for pair in pairs]
    
        for future in tqdm(as_completed(futures), total=len(futures)):
            result = future.result()
            if result:
                overlap.append(result)

    if not overlap:
        print("No overlapping segments found")
        return 0

    # since the results dont come back in the order they were pushed in, sort them again    
    segments = []
    for item in overlap:
        start = re.search(pattern, item.split(', ')[0]).start()
        segments.append({
            'prefix': item[:start],
            'suffix': item[start:],
            'contents': item
        })
    segments = [x['contents'].split(', ') for x in sorted(segments, key=lambda i: i['suffix'])]

    for s in segments:
        print(s)
    
    # now that we have the overlapping files we need to find the 
    # starts of each overlappig segment so we can truncate the file
    # and switch from one file stream to the next
    print(f"first segment starts at {segments[0]}")

    segpairs = [x for x in zip(segments[:-1], segments[1:])]
    if not segpairs:
        print("There is only one overlapping segment")
        deoverlap(segments, streams, inpath, outpath, copy=args.copy)
        return 0

    overlap_segments = [segments[0]]
    is_continuing = True
    for idx, segpair in enumerate(segpairs):
        if idx == 0:
            continue
        if is_continuing:
            if segpair[0][1] != segpair[1][0]:
                print(f"segment ends at {segpair[0][1]}")
                is_continuing = False
        else:
            print(f"segment starts at {segpair[0][0]} - {segpair[0][1]}")
            overlap_segments.append(segpair[0])
            is_continuing = True
    print(f"segment ends at {segpairs[-1][-1][-1]}")
    deoverlap(overlap_segments, streams, inpath, outpath, copy=args.copy)

    return 0

if __name__ == "__main__":
    sys.exit(main())
