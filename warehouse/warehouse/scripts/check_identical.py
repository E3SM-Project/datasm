import sys
import os
import argparse
from tqdm import tqdm
from pathlib import Path
from math import isclose
from numpy import allclose, subtract, not_equal, array_equal, where
from warehouse.util import con_message


def parse_args():
    parser = argparse.ArgumentParser(
        description="Check that the contents of two files are identical")
    parser.add_argument('file_one', type=str,
                        help="Path a netCDF files")
    parser.add_argument('file_two', type=str,
                        help="Path a netCDF files")
    parser.add_argument('--var-list', nargs="*",
                        default=["all"],
                        help="Variables to check, default is all")
    parser.add_argument('--exclude', nargs="*",
                        default=["lat", "lon", "area", "hyam", "hybm",
                                 "hyai", "hybi", "date_written", "time_written"],
                        help="Variables to check, default is all")
    return parser.parse_args()


def main():
    parsed_args = parse_args()
    import xarray as xr

    file_one = Path(parsed_args.file_one)
    file_two = Path(parsed_args.file_two)
    vars_to_check = parsed_args.var_list

    if not file_one.exists() or not file_two.exists():
        con_message("error","One of more input files does not exist")
        raise ValueError("One of more input files does not exist")

    data1 = xr.open_dataset(str(file_one.resolve()), decode_times=False)
    data2 = xr.open_dataset(str(file_two.resolve()), decode_times=False)

    all_match = True
    dont_match = []
    for variable in tqdm(data1.data_vars):

        if ("all" not in vars_to_check and variable not in vars_to_check) \
        or "bnds" in variable \
        or variable in parsed_args.exclude:
            continue

        a = data1[variable].load().values
        b = data2[variable].load().values
        if not allclose(a, b, equal_nan=True):
            dont_match.append(f"Values do not match for {variable}")
            all_match = False

    data1.close()
    data2.close()

    if all_match:
        con_message("info","All variables match")
        return 0

    con_message("warning","Some variables do not match")
    
    for m in dont_match:
        con_message("debug",m)
    return 1


if __name__ == "__main__":
    sys.exit(main())
