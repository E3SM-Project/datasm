import sys
import os
import argparse
import xarray as xr
from tqdm import tqdm
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import operator
from datasm.util import con_message

from dataclasses import dataclass


def parse_args():
    parser = argparse.ArgumentParser(
        description="Check that the time units match for every file in the dataset"
    )
    parser.add_argument(
        "input", type=str, help="Path to a directory containing a single dataset"
    )
    parser.add_argument(
        "--time-name",
        type=str,
        default="time",
        help="The name of the time axis, default is 'time'",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="suppress status bars and console output",
    )
    parser.add_argument(
        "-p", "--processes", type=int, default=8, help="number of parallel processes"
    )
    return parser.parse_args()


def check_file(idx, filepath, timename):
    """
    Return the time units from the file at the path
    """
    with xr.open_dataset(filepath, decode_times=False) as ds:
        # will return None if there aren't any units
        return idx, ds[timename].attrs.get("units")


@dataclass
class FileItem:
    units: str
    path: str


def main():
    parsed_args = parse_args()

    # populate and sort the list of FileItems
    # the list will be sorted based on its name
    futures = []
    files = sorted(
        [
            FileItem(units=None, path=str(x.resolve()))
            for x in Path(parsed_args.input).glob("*.nc")
        ],
        key=operator.attrgetter("path"),
    )

    # setup a process pool, then iterate of all the files
    # for each file submit a job, and get a future, for its units

    # once the future returns, stick its output into its
    # corresponding position in the output array
    with ProcessPoolExecutor(max_workers=parsed_args.processes) as pool:
        for idx, item in enumerate(files):
            futures.append(
                pool.submit(check_file, idx, item.path, parsed_args.time_name)
            )

        pbar = tqdm(disable=parsed_args.quiet, total=len(files))
        for future in as_completed(futures):
            pbar.update(1)
            idx, units = future.result()
            files[idx].units = units
        pbar.close()

    # walk through the files in order
    # the first file we find that doesnt match the expected units
    # is the first one in the bad batch
    # the offset output should be equal to the time of the first bad file
    # with the expected value (previous file end + freq)
    expected_units = files[0].units
    for idx, info in enumerate(files):
        if expected_units != files[idx].units:
            # we load the values of the previous file
            with xr.open_dataset(files[idx - 1].path, decode_times=False) as ds:
                freq = ds["time_bnds"].values[0][1] - ds["time_bnds"].values[0][0]
                prev_segment_end = ds["time"].values[-1] + freq
            # now we load the current file to get its first time value
            with xr.open_dataset(files[idx].path, decode_times=False) as ds:
                cur_segment_start = ds["time"].values[0]

            # we assume that the second file is always going to have a LOWER time value
            offset = prev_segment_end - cur_segment_start
            message = f"correct_units={expected_units},offset={offset}"
            if messages_path := os.environ.get("message_file"):
                with open(messages_path, "w") as outstream:
                    outstream.write(message.replace(":", "^"))
            else:
                con_message("error", "could not obtain message_path from environment")
                con_message(
                    "error", message
                )  # no idea if this should be info, warning or error
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
