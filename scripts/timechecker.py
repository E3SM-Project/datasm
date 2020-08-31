import os
import sys
import re
import argparse
import xarray as xr
import numpy as np
import netCDF4
from tqdm import tqdm
from datetime import datetime

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help="Directory path containing dataset")

    inpath = parser.parse_args().input

    # collect all the files and sort them by their date stamp
    names = [os.path.join(os.path.abspath(inpath), x) for x in os.listdir(inpath) if x.endswith('.nc')]
    pattern = r'\d{4}-\d{2}-\d{2}'
    fileinfo = []
    for name in names:
        start = re.search(pattern, name).start()
        fileinfo.append({
            'prefix': name[:start],
            'suffix': name[start:],
            'name': name
        })
    files = [x['name'] for x in sorted(fileinfo, key=lambda i: i['suffix'])]
    del fileinfo

    with xr.open_dataset(files[0], decode_times=False) as ds:
        freq = ds['time'][1].values.item() - ds['time'][0].values.item()
        print(f"Time frequency detected as {freq}")

    issues = None
    prevtime = None
    for file in tqdm(files):
        with xr.open_dataset(file, decode_times=False) as ds:
            for step in ds['time']:
                time = step.values.item()
                if not prevtime:
                    prevtime = time
                    continue
                delta = time - prevtime
                if delta != freq:
                    issues = True
                    print(f"time discontinuity in {file} at {time}, delta was {delta} when it should have been {freq}")    
                prevtime = time
    if not issues:
        print("No time index issues found.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
