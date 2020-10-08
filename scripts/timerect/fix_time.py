import os
from sys import exit
import argparse
from tqdm import tqdm
import cdms2
import xarray as xr
from subprocess import Popen, PIPE
from concurrent.futures import ProcessPoolExecutor, as_completed


def fix_units(path, time_units, dryrun, script_path):

    fp = cdms2.open(path, 'r+')
    time = fp.getAxis('time')

    if time.units != time_units:
        if dryrun:
            print(f"Found issue with time units {time.units} -> {time_units}")
            fp.close()
        else:
            time.units = time_units
            fp.close()

        if dryrun:
            print(' '.join(['ncap2', '-A', '-S', script_path, path]))
        else:
            cmd = ['ncap2', '-A', '-S', script_path, path]
            proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
            out, err = proc.communicate()
            if err:
                print(out.decode('utf-8'))
                print(err.decode('utf-8'))
                return 1
    else:
        fp.close()

def get_bounds_name(file):
    with xr.open_dataset(file, decode_times=False) as ds:
        if 'time_bnds' in ds.data_vars:
            return 'time_bnds'
        elif 'time_bounds' in ds.data_vars:
            return 'time_bounds'
        else:
            raise ValueError("Unrecognized time bounds name")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input', nargs="+", help="paths to directories containing data with incorrect time units")
    parser.add_argument('-t', '--time-offset', required=True)
    parser.add_argument('-d', '--time-units', default="days since 1850-01-01 00:00:00")
    parser.add_argument('--dryrun', action="store_true", default=False)
    parser.add_argument('-p', '--processes', default=6, type=int)
    args = parser.parse_args()

    with ProcessPoolExecutor(max_workers=args.processes) as pool:
        for comp in args.input:
            files = os.listdir(comp)
            futures = []
            bounds = get_bounds_name(os.path.join(comp, files[0]))
            
            script_path = "ncap2_script.nco"
            with open(script_path, 'w') as fp:
                fp.write(f'offset={args.time_offset};time(:)=time(:)+offset;{bounds}(:,:)={bounds}(:,:)+offset;')
            
            for f in files:
                path = os.path.join(comp, f)
                futures.append(
                    pool.submit(
                        fix_units,
                        path,
                        args.time_units,
                        args.dryrun,
                        script_path))

            for _ in tqdm(as_completed(futures), total=len(files), desc=comp):
                pass

    return 0


if __name__ == "__main__":
    exit(main())