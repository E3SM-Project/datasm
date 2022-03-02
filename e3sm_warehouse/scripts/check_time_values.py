import os
import sys
import re
import argparse
import xarray as xr
import numpy as np
import netCDF4
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from e3sm_warehouse.util import con_message

calendars = {
    "noleap": {
        1: 31,
        2: 28,
        3: 31,
        4: 30,
        5: 31,
        6: 30,
        7: 31,
        8: 31,
        9: 30,
        10: 31,
        11: 30,
        12: 31,
    }
}


def get_time_units(path):
    time_names = ["Time", "time"]
    time_name = ""
    with xr.open_dataset(path, decode_times=False) as ds:
        for name in time_names:
            if name in ds.dims:
                time_name = name
                break
        return ds[time_name].attrs["units"], time_name


def get_month(path):
    pattern = r"\d{4}-\d{2}"
    s = re.search(pattern, path)
    if not s:
        con_message("error", f"Unable to find month string for {path}")
        sys.exit(1)
    return int(path[s.start() + 5 : s.start() + 7])


def check_file(file, freq, idx, time_name="time"):
    """
    Step through the file checking that each step in time is exactly how long it should be
    and that the time index is monotonically increasing
    """
    prevtime = None
    first, last = None, None
    with xr.open_dataset(file, decode_times=False) as ds:
        if len(ds[time_name]) == 0:
            return None, None, idx
        for step in ds[time_name]:
            time = step.values.item()
            if not prevtime:
                prevtime = time
                first = time
                continue
            delta = time - prevtime
            if delta == 0:
                # monthly data
                return time, time, idx
            elif delta != freq:
                con_message(
                    "warning",
                    f"time discontinuity in {file} at {time}, delta was {delta} when it should have been {freq}",
                )
            prevtime = time
        last = time
    return first, last, idx


def main():
    parser = argparse.ArgumentParser(
        description="Check a directory of raw E3SM time-slice files for discontinuities in the time index"
    )
    parser.add_argument("input", help="Directory path containing dataset")
    parser.add_argument(
        "-j",
        "--jobs",
        default=8,
        type=int,
        help="the number of processes, default is 8",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        default=False,
        help="Disable progress-bar for batch/background processing",
    )
    args = parser.parse_args()
    inpath = args.input

    con_message("info", f"Running timechecker:dataset={inpath}")

    # collect all the files and sort them by their date stamp
    names = [
        os.path.join(os.path.abspath(inpath), x)
        for x in os.listdir(inpath)
        if x.endswith(".nc")
    ]
    pattern = r"\d{4}-\d{2}"
    fileinfo = []
    for name in names:
        start = re.search(pattern, name).start()
        if not start:
            con_message(
                "error",
                f"The year stamp search pattern {pattern} didn't find what it was expecting",
            )
            sys.exit(1)
        fileinfo.append({"prefix": name[:start], "suffix": name[start:], "name": name})
    files = [x["name"] for x in sorted(fileinfo, key=lambda i: i["suffix"])]
    del fileinfo

    time_units, time_name = get_time_units(files[0])

    monthly = False
    freq = None
    # find the time frequency by checking the delta from the 0th to the 1st step
    # import ipdb; ipdb.set_trace()
    with xr.open_dataset(files[0], decode_times=False) as ds:
        freq = ds.attrs.get("time_period_freq")

        if freq == "month_1":
            monthly = True
            con_message("info", "Found monthly data")
            calendar = ds[time_name].attrs["calendar"]
            if calendar not in calendars:
                con_message("error", f"Unsupported calendar type {calendar}")
                sys.exit(1)
        elif freq is None:
            if ds.attrs.get("title") == "CLM History file information":
                monthly = True
            calendar = ds[time_name].attrs["calendar"]
            if calendar not in calendars:
                con_message("error", f"Unsupported calendar type {calendar}")
                sys.exit(1)
        else:
            con_message("info", "Found sub-monthly data")
            freq = ds[time_name][1].values.item() - ds[time_name][0].values.item()
            con_message("info", f"Detected frequency: {freq}, with units: {time_units}")

    # iterate over each of the files and get the first and last index from each file
    issues = list()
    prevtime = None
    indices = [None for _ in range(len(files))]
    with ProcessPoolExecutor(max_workers=args.jobs) as pool:
        futures = [
            pool.submit(check_file, file, freq, idx) for idx, file in enumerate(files)
        ]

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Checking time indices",
            disable=args.quiet,
        ):
            first, last, idx = future.result()
            indices[idx] = (first, last, idx)

    prev = None
    for first, last, idx in indices:
        if not prev:
            prev = last
            continue
        if monthly:
            month = get_month(files[idx])
            target = prev + calendars[calendar][month]
        else:
            target = prev + freq
        if not first or not last:
            # this file had an empty index, move on and start checking the next one as though this one was there
            msg = f"Empty time index found in {os.path.basename(files[idx])}"
            issues.append(msg)
            prev = target
            continue
        if first != target:
            msg = f"index issue file: {os.path.basename(files[idx])} has index {(first, last)} should be ({target, last}), the start index is off by ({first - target}) {time_units.split(' ')[0]}. "
            issues.append(msg)
        prev = last

    if issues:
        issues.append(f"Result=Fail:dataset={inpath}")
        for msg in issues:
            con_message("warning", msg)
        return 1

    con_message("info", "No time index issues found.")
    con_message("info", f"Result=Pass:dataset={inpath}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
