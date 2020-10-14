import os
import sys
import re
import argparse
import xarray as xr
import numpy as np
import netCDF4
from tqdm import tqdm
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed


def get_time_units(path):
    with xr.open_dataset(path, decode_times=False) as ds:
        return ds['time'].attrs['units']

def check_file(file, freq, idx):
    """
    Step through the file checking that each step in time is exactly how long it should be
    and that the time index is monotonically increasing
    """
    prevtime = None
    first, last = None, None
    with xr.open_dataset(file, decode_times=False) as ds:
        for step in ds['time']:
            time = step.values.item()
            if not prevtime:
                prevtime = time
                first = time
                continue
            delta = time - prevtime
            if delta != freq:
                issues = True
                print(f"time discontinuity in {file} at {time}, delta was {delta} when it should have been {freq}")    
            prevtime = time
        last = time
    return first, last, idx

def main():
    parser = argparse.ArgumentParser(description="Check a directory of raw E3SM time-slice files for discontinuities in the time index")
    parser.add_argument('input', help="Directory path containing dataset")
    parser.add_argument('-j', '--jobs', default=8, type=int, help="the number of processes, default is 8")
    args = parser.parse_args()
    inpath = args.input

    # collect all the files and sort them by their date stamp
    names = [os.path.join(os.path.abspath(inpath), x) for x in os.listdir(inpath) if x.endswith('.nc')]
    pattern = r'\d{4}-\d{2}'
    fileinfo = []
    for name in names:
        start = re.search(pattern, name).start()
        if not start:
            raise ValueError(f"The year stamp search pattern {pattern} didnt find what it was expecting")
        fileinfo.append({
            'prefix': name[:start],
            'suffix': name[start:],
            'name': name
        })
    files = [x['name'] for x in sorted(fileinfo, key=lambda i: i['suffix'])]
    del fileinfo

    time_units = get_time_units(files[0])

    # find the time frequency by checking the delta from the 0th to the 1st step
    with xr.open_dataset(files[0], decode_times=False) as ds:
        freq = ds['time'][1].values.item() - ds['time'][0].values.item()
        print(f"Time frequency detected as {freq} {time_units}")

    # iterate over each of the files and get the first and last index from each file
    issues = list()
    prevtime = None
    indices = [None for _ in range(len(files))]
    with ProcessPoolExecutor(max_workers=args.jobs) as pool:
        futures = [pool.submit(check_file, file, freq, idx) for idx, file in enumerate(files)]

        for future in tqdm(as_completed(futures), total=len(futures), desc="Checking time indices"):
            first, last, idx = future.result()
            indices[idx] = (first, last, idx)
    
    import ipdb; ipdb.set_trace()
    prev = None
    for first, last, idx in indices:
        if not prev:
            prev = last
            continue
        target = prev + freq
        if first != target:
            msg = f"index issue file: {files[idx]} has index {(first, last)} should be ({target, last}), the start index is off by ({first - target}) {time_units.split(' ')[0]}. "
            issues.append(msg)
        prev = last

    if not issues:
        print("No time index issues found.")
    else:
        [print(msg) for msg in issues]
    return 0

if __name__ == "__main__":
    sys.exit(main())
