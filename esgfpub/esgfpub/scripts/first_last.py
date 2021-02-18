import sys
import argparse
import xarray as xr

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('path')
    parser.add_argument('--all', action="store_true")
    args = parser.parse_args()

    with xr.open_dataset(args.path, decode_times=False) as ds:
        if args.all:
            for bounds in ds['time_bnds']:
                print(f"{bounds.values}")
        else:
            print(f"{ds['time_bnds'][0].values[0]} - {ds['time_bnds'][-1].values[-1]}")
    return 0

if __name__ == "__main__":
    sys.exit(main())