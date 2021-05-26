import os
import sys
import argparse
import xarray as xr

from tqdm import tqdm
from datetime import datetime


def put_message(message):
    print(f'{datetime.now().strftime("%Y%m%d_%H%M%S")}:{message}')


def check_file(file, freq, time_name='time'):
    """
    Step through the file checking that each step in time is exactly how long it should be
    and that the time index is monotonically increasing
    """
    prevtime = None
    first, last = None, None
    with xr.open_dataset(file, decode_times=False) as ds:
        if len(ds[time_name]) == 0:
            return None, None
        for step in ds[time_name]:
            time = step.values.item()
            if not prevtime:
                prevtime = time
                first = time
                continue
            delta = time - prevtime
            if delta != freq:
                put_message(
                    f"time discontinuity in {file} at {time}, delta was {delta} when it should have been {freq}")
            prevtime = time
        last = time
    return first, last, file


def main():
    parser = argparse.ArgumentParser(
        description="Check a directory of CMIP6 timeseries files for discontinuities in the time index")
    parser.add_argument(
        'input', 
        help="Directory path containing dataset")
    args = parser.parse_args()
    inpath = args.input

    put_message(f'Running timechecker:dataset={inpath}')

    # collect all the files and sort them by their date stamp
    files = sorted([os.path.join(os.path.abspath(inpath), x)
                    for x in os.listdir(inpath) if x.endswith('.nc')])

    freq = None
    # find the time frequency by checking the delta from the 0th to the 1st step
    with xr.open_dataset(files[0], decode_times=False) as ds:
        time_units = ds['time'].attrs['units']
        
        freq = ds['time'][1].values.item() - ds['time'][0].values.item()
        put_message(f"Detected frequency: {freq}, with units: {time_units}")
    


    # iterate over each of the files and get the first and last index from each file
    indices = [check_file(file, freq) for file in tqdm(files)]

    prev = None
    issue = False
    for first, last, file in indices:
        if not prev:
            prev = last
            continue
        target = prev + freq
        if first != target:
            msg = f"index issue file: {file} - {(first, last)} should be ({target, last}), the start index is off by ({first - target}) {time_units.split(' ')[0]}. "
            put_message(msg)
            issue = True
        prev = last
    if not issue:
        put_message("No time index issues found.")
        put_message(f"Result=Pass:dataset={inpath}")
    else:
        put_message(f"Result=Fail:dataset={inpath}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
