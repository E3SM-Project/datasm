"""
Generate an ESGF mapfile from a directory, using the user supplied dataset id

Example ESGF mapfile entry:

E3SM.1_0.1950-Control.0_25deg_atm_18-6km_ocean.atmos.native.model-output.6hr.ens1#1 | /p/user_pub/work/E3SM/1_0/1950-Control/0_25deg_atm_18-6km_ocean/atmos/native/model-output/6hr/ens1/v1/theta.20180906.branch_noCNT.A_WCYCL1950S_CMIP6_HR.ne120_oRRS18v3_ICG.cam.h3.0027-07-15-21600.nc | 1913443680 | mod_time=1543049079.0 | checksum=45a234640fadcc53c8ffdfe82114bc0d5674b7b84f4c773a9e1b16a77ddeeab7 | checksum_type=SHA256

"""

import sys
import os
import argparse
from tqdm import tqdm
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from subprocess import Popen, PIPE
import hashlib


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate an ESGF mapfile from a given directory, with the given dataset_id")
    parser.add_argument(
        'input', type=str,
        help="Path a directory full of netCDF files")
    parser.add_argument(
        'dataset_id', type=str,
        help="The ESGF dataset id")
    parser.add_argument(
        'version_number', type=int,
        help="The version number of the dataset")
    parser.add_argument(
        '--outpath',
        type=str,
        help="Output path for the mapfile including the file name, " 
             "by default it will be named <dataset_id>.map and be placed "
             "in the current working directory.")
    parser.add_argument(
        '-p', '--processes',
        type=int, default=8,
        help="Number of parallel jobs, default is 8")
    return parser.parse_args()


def hash_file(filepath: Path):

    sha256 = hashlib.sha256()
    fullpath = str(filepath.resolve())
    # handle content in binary form
    with open(fullpath, "rb") as instream:
        while chunk := instream.read(4096):
            sha256.update(chunk)

    return sha256.hexdigest(), fullpath


def main():
    parsed_args = parse_args()

    input_path = Path(parsed_args.input)
    dataset_id = parsed_args.dataset_id
    version_nm = parsed_args.version_number
    numberproc = parsed_args.processes

    if not input_path.exists() or not input_path.is_dir():
        raise ValueError("Input directory does not exist or is not a directory")

    outpath = parsed_args.outpath
    if outpath:
        outpath = Path(outpath)
    else:
        outpath = Path(f"{dataset_id}.map")

    futures = []
    pool = ProcessPoolExecutor(max_workers=numberproc)
    for path in input_path.glob('*.nc'):
        futures.append(
            pool.submit(
                hash_file, path))

    with open(outpath, 'w') as outstream:
        try:
            for future in tqdm(as_completed(futures), total=len(futures)):
                filehash, pathstr = future.result()
                filestat = Path(pathstr).stat()
                line = f"{dataset_id}#{version_nm} | {pathstr} | {filestat.st_size} | mod_time={filestat.st_mtime} | checksum={filehash} | checksum_type=SHA256\n"
                outstream.write(line)

        except KeyboardInterrupt:
            print("Cause keyboard interrupt, exiting. Mapfile will be incomplete")
            for future in futures:
                future.cancel()
            return 1
        except Exception as e:
            print(e)
            return 1
    
    message = f"mapfile_path={outpath}"
    if (messages_path := os.environ.get('message_file')):
        with open(messages_path, 'w') as outstream:
            outstream.write(message)
    else:
        print(message)
    return 0


if __name__ == "__main__":
    sys.exit(main())