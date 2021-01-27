import sys
import argparse
import xarray as xr
from tqdm import tqdm
from pathlib import Path

def parse_args():
    parser = argparse.ArgumentParser(
        description="Check that the time units match for every file in the dataset")
    parser.add_argument('input', type=str,
                        help="Path to a directory containing a single dataset")
    parser.add_argument('--time-name', type=str, default='time',
                        help="The name of the time axis, default is 'time'")
    parser.add_argument('-q', '--quiet', action="store_true",
                        help="suppress status bars and console output")
    return parser.parse_args()


def check_file(filepath, timename):
    """
    Return the time units from the file at the path
    """
    with xr.open_dataset(filepath, decode_times=False) as ds:
        # will return None if there aren't any units
        return ds[timename].attrs.get('units')


def main():
    parsed_args = parse_args()
    units = []
    if not parsed_args.quiet:
        total = 0
        for _ in Path(parsed_args.input).glob('*.nc'):
            total += 1
    else:
        total = None

    pbar = tqdm(Path(parsed_args.input).glob('*.nc'), disable=parsed_args.quiet, total=total)
    for item in pbar:
        if not item.exists():
            continue
        if not parsed_args.quiet:
            desc = f"Checking {item.name}"
            pbar.set_description(desc)
        units.append(check_file(item.resolve(), parsed_args.time_name))
    for unit in units:
        if unit != units[0]:
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
