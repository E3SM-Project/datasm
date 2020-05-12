import sys
import os
import argparse
import xarray as xr
from tqdm.auto import tqdm
from datetime import datetime

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', dest="variable", required=True, help="the variable name")
    parser.add_argument('-n', dest="attribute_name", required=True, help="the variable attribute name")
    parser.add_argument('-a', dest="attribute_value", required=True, help="the variable attribute value")
    parser.add_argument('path', help="the variable name")
    return parser.parse_args()

def main():
    args = parse_args()

    now = datetime.now()
    new_version_name = f"v{now.year:04d}{now.month:02d}{now.day:02d}"
    new_version_path = os.path.join(os.sep.join(args.path.split(os.sep)[:-2]), new_version_name)
    if not os.path.exists(new_version_path):
        os.makedirs(new_version_path)

    for filename in tqdm(os.listdir(args.path)):
        ds = xr.open_dataset(f'{args.path}/{filename}')
        ds[args.variable].attrs[args.attribute_name] = args.attribute_value
        ds.to_netcdf(f'{new_version_path}/{filename}')
        ds.close()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())