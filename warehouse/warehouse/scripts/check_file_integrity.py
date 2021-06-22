import sys
import os
import argparse
from tqdm import tqdm
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from subprocess import Popen, PIPE
from warehouse.util import con_message


def parse_args():
    parser = argparse.ArgumentParser(
        description="Check that the contents of two files are identical"
    )
    parser.add_argument("input", type=str, help="Path a directory full of netCDF files")
    parser.add_argument(
        "-p",
        "--processes",
        type=int,
        default=8,
        help="Number of parallel jobs, default is 8",
    )
    return parser.parse_args()


def check_file(path):
    # import ipdb;ipdb.set_trace()
    cmd = f"ncdump -h {path}".split()
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    out = out.decode("utf-8")
    err = err.decode("utf-8")

    if not out or "NetCDF: HDF error" in err:
        con_message("error", f"Error loading {path}")
        return 1
    return 0


def main():
    parsed_args = parse_args()

    input_path = Path(parsed_args.input)

    if not input_path.exists() or not input_path.is_dir():
        con_message("error", f"Input directory does not exist or is not a directory")
        return 1
    futures = []
    pool = ProcessPoolExecutor(max_workers=parsed_args.processes)
    for path in input_path.glob("*.nc"):
        realpath = str(path.resolve())
        futures.append(pool.submit(check_file, realpath))

    error = False
    try:
        for future in tqdm(as_completed(futures), total=len(futures)):
            result = future.result()
            if result == 1:
                error = True
    except KeyboardInterrupt:
        for future in futures:
            future.cancel()

    if error:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
