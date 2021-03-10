import sys
import os
import argparse
import shutil
import subprocess
import time
from pathlib import Path
from datetime import datetime

# gv_logname = ''

'''
    Usage:  validate_mapfile --data-path version_path --mapfile mapfile_path

    Returns 0 if every file in version path is listed in the mapfile
'''

def parse_args():
    parser = argparse.ArgumentParser(
        description="Ensure every datafile in supplied data-path exists in the given mapfile.")
    parser.add_argument(
        '--data-path', 
        type=str, 
        dest='datapath', 
        required=True, 
        help="source directory of netCDF files to seek in mapfile")
    parser.add_argument(
        '--mapfile', 
        type=str, 
        required=True, 
        help="mapfile to be validated")
    return parser.parse_args()


def loadFileLines(file: str):
    retlist = []
    filepath = Path(file)
    if not filepath.exists():
        raise ValueError(f"Cannot load lines from file {filepath} as it does not exist")
    
    with open(filepath.resolve(), "r") as instream:
        retlist = [x for x in instream.read().split('\n') if x[:-1]]
    return retlist


def validate_mapfile(mapfile: str, srcdir: Path):
    ''' 
    at this point, the srcdir should contain the datafiles (*.nc)
    and the parent dir/.mapfile, so we can do a name-by-name comparison.
    MUST test for each srcdir datafile in mapfile listing.

    Params:
        mapfile (str): the string path to the mapfile
        srcdir (Path): a Path object pointint to the directory containing the data files
    Returns:
        True if the mapfile is valid, False otherwise
    '''
    dataset_files = sorted([x for x in srcdir.glob('*.nc')])
    mapfile_lines = sorted(loadFileLines(mapfile))

    if not len(dataset_files) == len(mapfile_lines):
        raise ValueError("Number of files does not match number of entries in the mapfile")
        
    # MUST assume both lists sort identically - O(n) > O(n^2)
    pairlist = list(zip(dataset_files, mapfile_lines))
    for file, mapentry in pairlist:
        if file not in mapentry: 
            return False

    return True


def main():

    parsed_args = parse_args()

    if validate_mapfile(parsed_args.mapfile, parsed_args.datapath):
        return 0
    else:
        return 1
 
if __name__ == "__main__":
  sys.exit(main())



