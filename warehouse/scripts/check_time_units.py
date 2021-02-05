import sys
import os
import argparse
import xarray as xr
from tqdm import tqdm
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import operator

from dataclasses import dataclass

def parse_args():
    parser = argparse.ArgumentParser(
        description="Check that the time units match for every file in the dataset")
    parser.add_argument('input', type=str,
                        help="Path to a directory containing a single dataset")
    parser.add_argument('--time-name', type=str, default='time',
                        help="The name of the time axis, default is 'time'")
    parser.add_argument('-q', '--quiet', action="store_true",
                        help="suppress status bars and console output")
    parser.add_argument('-p', '--processes', type=int, default=8,
                        help="number of parallel processes")
    return parser.parse_args()


def check_file(idx, filepath, timename):
    """
    Return the time units from the file at the path
    """
    with xr.open_dataset(filepath, decode_times=False) as ds:
        # will return None if there aren't any units
        return idx, ds[timename].attrs.get('units')

@dataclass
class FileItem:
    units: str
    path: str

def main():
    parsed_args = parse_args()
    
    futures = []
    files = sorted([
        FileItem(units=None, path=str(x.resolve()))
        for x in Path(parsed_args.input).glob('*.nc')],
        key=operator.attrgetter('path'))

    with ProcessPoolExecutor(max_workers=parsed_args.processes) as pool:
        for idx, item in enumerate(files):
            futures.append(
                pool.submit(
                    check_file, idx, item.path, parsed_args.time_name))

        pbar = tqdm(disable=parsed_args.quiet, total=len(files))
        for future in as_completed(futures):
            pbar.update(1)
            idx, units = future.result()
            files[idx].units = units
        pbar.close()

    expected_units = files[0].units
    for idx, info in enumerate(files):
        if expected_units != files[idx].units:
            with xr.open_dataset(files[idx-1].path, decode_times=False) as ds:
                freq = ds['time_bnds'].values[0][1] - ds['time_bnds'].values[0][0]
                offset = ds['time'].values[-1] + freq
            message = f"correct_units={expected_units},offset={offset}"
            if (messages_path := os.environ.get('MESSAGES_FILE')):
                with open(messages_path, 'w') as outstream:
                    outstream.write(message.replace(':', '^'))
            else:
                print(message)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
