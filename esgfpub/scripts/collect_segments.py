import os
import sys
import re
import argparse
import xarray as xr
import numpy as np
import netCDF4
from tqdm import tqdm
from shutil import move as move_file
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

def get_indices(path):
    with xr.open_dataset(path, decode_times=False) as ds:
        return path, ds['time_bnds'][0].values[0], ds['time_bnds'][-1].values[-1]

def find_segments(inpath, num_jobs):
    

    print("starting segment collection")
    # collect all the files and sort them by their date stamp
    names = [os.path.join(inpath, x) for x in os.listdir(inpath) if x.endswith('.nc')]
    pattern = r'\d{4}-\d{2}-\d{2}'
    files = []
    for name in names:
        start = re.search(pattern, name).start()
        files.append({
            'suffix': name[start:],
            'name': name
        })
    files = [x['name'] for x in sorted(files, key=lambda i: i['suffix'])]

    with ProcessPoolExecutor(max_workers=num_jobs) as pool:
        futures = [pool.submit(get_indices, x) for x in files]
    
        file_info = []
        for future in tqdm(as_completed(futures), desc="Collecting time indices", total=len(futures)):
            name, start_index, end_index = future.result()
            file_info.append({
                'name': name,
                'start': start_index,
                'end': end_index
            })
    
    file_info.sort(key=lambda i: i['start'])

    # prime the segments by adding the first file
    
    f1 = file_info.pop(0)
    segments = {(f1['start'], f1['end']): [f1['name']]}
    # import ipdb; ipdb.set_trace()

    while len(file_info) > 0:

        file = file_info.pop(0)
        joined = False
        for segstart, segend in segments.keys():
            # the start of the file aligns with the end of the segment
            if segend == file['start']:
                segments[(segstart, file['end'])] = segments.pop((segstart, segend)) + [file['name']]
                joined = True
                break
            # the end of the file aligns with the start of the segment
            elif segstart == file['end']:
                segments[(file['start'], segend)] = [file['name']] + segments.pop((segstart, segend))
                joined = True
                break
        if not joined:
            if file['start'] == 0.0:
                raise ValueError(f"the file {file['name']} has a start index of 0.0")
            if segments.get((file['start'], file['end'])):
                raise ValueError(f"the file {file['name']} has perfectly matching time indices with the previous segment {segments.get((file['start'], file['end']))}")
            segments[(file['start'], file['end'])] = [file['name']]
    
    for seg in segments.keys():
        print(seg, len(segments[seg]))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help="The directory to check for time index issues, should only contain a single time-frequency from a single case")
    parser.add_argument('-j', '--jobs', default=8, type=int, help="the number of processes, default is 8")
    args = parser.parse_args()
    inpath = args.input
    num_jobs = args.jobs

    find_segments(inpath, num_jobs)

if __name__ == "__main__":
    sys.exit(main())
