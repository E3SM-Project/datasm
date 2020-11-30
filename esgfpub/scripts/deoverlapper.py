import os
import sys
import argparse
import xarray as xr


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--first-block', help="First file to pull data from")
    parser.add_argument('--second-block', help="second file, only used to find the stopping time index")
    parser.add_argument('--output-file', help="name of the file to drop the truncated dataset into")
    args = parser.parse_args()

    with xr.open_dataset(args.second_block, decode_times=False) as ds:
        end_index = ds['time_bnds'][0].values[0]
    

    new_ds = xr.Dataset()
    with xr.open_dataset(args.first_block, decode_times=False) as ds:
        target_index = 0
        for i in range(0, 100000):
            if ds['time_bnds'][i].values[0] == end_index:
                break
            target_index += 1

        new_ds.attrs = ds.attrs
        for variable in ds.data_vars:
            if 'time' not in ds[variable].coords:
                new_ds[variable] = ds[variable]
                continue
            new_ds[variable] = ds[variable].isel(time=slice(0, target_index))
            new_ds[variable].attrs = ds[variable].attrs
    
    new_ds.to_netcdf(args.output_file)

    return 0


if __name__ == "__main__":
    sys.exit(main())