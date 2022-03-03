import sys
import os
import argparse
import shutil
import subprocess
import time
from pathlib import Path
from tqdm import tqdm
from warehouse.util import con_message


"""
    Usage:  validate_mapfile --data-path version_path --mapfile mapfile_path

    Returns 0 if every file in version path is listed in the mapfile
"""


def parse_args():
    parser = argparse.ArgumentParser(
        description="Ensure every datafile in supplied data-path exists in the given mapfile."
    )
    parser.add_argument(
        "--data-path",
        type=str,
        dest="datapath",
        required=True,
        help="source directory of netCDF files to seek in mapfile",
    )
    parser.add_argument(
        "--mapfile", type=str, required=True, help="mapfile to be validated"
    )
    parser.add_argument(
        "-q", "--quiet", action="store_true", help="Dont display a progress bar"
    )
    return parser.parse_args()


def loadFileLines(filepath: Path):
    retlist = []
    if not filepath.exists():
        con_message(
            "error", f"Cannot load lines from file {filepath} as it does not exist"
        )
        sys.exit(1)

    with open(filepath.resolve(), "r") as instream:
        retlist = [Path(x.split("|")[1]).name for x in instream.readlines()]
    return retlist


def validate_mapfile(mapfile: str, srcdir: Path, quiet: bool):
    """
    at this point, the srcdir should contain the datafiles (*.nc)
    and the parent dir/dsid.map, so we can do a name-by-name comparison.
    MUST test for each srcdir datafile in mapfile listing.

    Params:
        mapfile (str): the string path to the mapfile
        srcdir (Path): a Path object pointint to the directory containing the data files
    Returns:
        True if the mapfile is valid, False otherwise
    """
    con_message("info", f"checking mapfile {mapfile}")

    dataset_files = sorted([x.name for x in srcdir.glob("*.nc")])
    mapfile_lines = sorted(loadFileLines(mapfile))

    if not len(dataset_files) == len(mapfile_lines):
        con_message(
            "error", "Number of files does not match number of entries in the mapfile"
        )
        sys.exit(1)

    # MUST assume both lists sort identically - O(n) > O(n^2)
    pairlist = list(zip(dataset_files, mapfile_lines))
    error = []
    # import ipdb; ipdb.set_trace()
    for file, mapentry in tqdm(pairlist, disable=quiet):
        if file not in mapentry:
            error.append(file)

    if error:
        for e in error:
            con_message("error", e)
        return False

    return True


def main():

    parsed_args = parse_args()

    success = validate_mapfile(
        Path(parsed_args.mapfile), Path(parsed_args.datapath), parsed_args.quiet
    )
    if success:
        if not parsed_args.quiet:
            con_message("info", "Mapfile includes all files")

        return 0
    else:
        if not parsed_args.quiet:
            con_message("error", "Mapfile is missing one or more files")
        return 1


if __name__ == "__main__":
    sys.exit(main())
