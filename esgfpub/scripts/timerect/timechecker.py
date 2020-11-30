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

calendars = {
    'noleap': {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
}

def get_time_units(path):
    with xr.open_dataset(path, decode_times=False) as ds:
        return ds['time'].attrs['units']

def get_month(path):
    pattern = r'\d{4}-\d{2}'
    s = re.search(pattern, path)
    if not s:
        raise ValueError(f"Unable to find month string for {path}")
    return int(path[s.start() + 5: s.start() + 7])

def check_file(file, freq, idx):
    """
    Step through the file checking that each step in time is exactly how long it should be
    and that the time index is monotonically increasing
    """
    prevtime = None
    first, last = None, None
    with xr.open_dataset(file, decode_times=False) as ds:
        if len(ds['time']) == 0:
            return None, None, idx
        for step in ds['time']:
            time = step.values.item()
            if not prevtime:
                prevtime = time
                first = time
                continue
            delta = time - prevtime
            if delta == 0:
                # monthly data
                return time, time, idx
            elif delta != freq:
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

    monthly = False
    freq = None
    # find the time frequency by checking the delta from the 0th to the 1st step
    with xr.open_dataset(files[0], decode_times=False) as ds:
        if ds.attrs.get("time_period_freq") == "month_1":
            monthly = True
            print("Found monthly data")
            calendar = ds['time'].attrs['calendar']
            if calendar not in calendars:
                raise ValueError(f"Unsupported calendar type {calendar}")
        else:
            print("Found sub-monthly data")
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
    
    prev = None
    for first, last, idx in indices:
        if not prev:
            prev = last
            continue
        if monthly:
            month = get_month(files[idx])
            target = prev + calendars[calendar][month]
        else:
                target = prev + freq
        if not first or not last:
            # this file had an empty index, move on and start checking the next one as though this one was there
            msg = f"Empty time index found in {files[idx]}"
            issues.append(msg)
            prev = target
            continue
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
