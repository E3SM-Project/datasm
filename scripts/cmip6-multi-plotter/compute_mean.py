import sys
import os
import argparse
import xarray as xr
        
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', required=True)
    args = parser.parse_args()

    ds = xr.open_mfdataset(f'{args.path}/*.nc', combine='by_coords')
    possible_dims = ['depth', 'lat', 'lon', 'plev', 'tau', 'lev', 'sector']    
    dims = tuple(x if x != 'sector' else 'basin' for x in ds.dims if x in possible_dims)
    variable = next(x for x in ds.data_vars if 'bnds' not in x and 'bounds' not in x)
    idx = args.path.find('CMIP6')
    dataset_id = args.path[idx:].replace(os.sep, '.')
    ds[variable].mean(dims).compute().to_netcdf(f"{dataset_id}.nc")

    return 0

if __name__ == "__main__":
    sys.exit(main())