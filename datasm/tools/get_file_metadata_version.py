import os
import sys
import argparse
from argparse import RawTextHelpFormatter
from pathlib import Path
import re
import xarray as xr

helptext = """
    Print "v<version_data>" from metadata of first file found in .nc files of supplied directory.
    If not found, return "NONE".
"""

def get_first_nc_file(ds_ver_path):
    dsPath = Path(ds_ver_path)
    for anyfile in dsPath.glob("*.nc"):
        return Path(dsPath, anyfile)

def get_dataset_version_from_file_metadata(latest_dir):  # input latest_dir already includes version leaf directory
    ds_path = Path(latest_dir)
    if not ds_path.exists():
        # print(f"No version: no path {latest_dir}")
        return 'NONE'

    first_file = get_first_nc_file(latest_dir)
    if first_file == None:
        # print(f"No first_file")
        return 'NONE'

    ds = xr.open_dataset(first_file)
    if 'version' in ds.attrs.keys():
        ds_version = ds.attrs['version']
        if not re.match(r"v\d", ds_version):
            # print(f"Invalid version {ds_version} in metadata")
            ds_version = 'NONE'
    else:
        # print(f"No version in ds.attrs.keys()")
        ds_version = 'NONE'
    return ds_version

def main():
    parser = argparse.ArgumentParser(
        description=helptext, prefix_chars="-", formatter_class=RawTextHelpFormatter
    )
    parser._action_groups.pop()
    required = parser.add_argument_group("required arguments")


    required.add_argument(
        "-i", "--inputdir", action="store", dest="src_dir", type=str, required=True
    )
    args = parser.parse_args()

    if not os.path.exists(args.src_dir):
        print(f"Error:  Specified input directory not found: {args.src_dir}")
        sys.exit(0)

    vers = get_dataset_version_from_file_metadata(args.src_dir)
    print(f"{vers}")

    sys.exit(0)

if __name__ == "__main__":
    sys.exit(main())

