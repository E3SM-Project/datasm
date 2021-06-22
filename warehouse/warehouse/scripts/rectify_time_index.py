import os
import sys
import re
import argparse
import xarray as xr
import numpy as np
import netCDF4
from pathlib import Path
from tqdm import tqdm
from shutil import move as move_file
from shutil import copyfile
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import combinations
from warehouse.util import con_message

# def get_indices(path, bndsname):
#     with xr.open_dataset(path, decode_times=False) as ds:
#         return path, ds[bndsname][0].values[0], ds[bndsname][-1].values[-1]


def filter_files(file_info):
    to_remove = []
    for combo in combinations(file_info, 2):
        if (
            combo[0]["start"] == combo[1]["start"]
            and combo[0]["end"] == combo[1]["end"]
        ):
            con_message("debug", f"{combo[0]['name']} == {combo[1]['name']}")
            _, n1 = os.path.split(combo[0]["name"])
            _, n2 = os.path.split(combo[1]["name"])
            if int(n1[:8]) < int(n2[:8]):
                to_remove.append(combo[0])
            else:
                to_remove.append(combo[1])
        elif combo[0]["start"] == combo[1]["start"]:
            if combo[0]["end"] < combo[1]["end"]:
                to_remove.append(combo[0])
            else:
                to_remove.append(combo[1])

    for i1 in to_remove:
        for idx, i2 in enumerate(file_info):
            if i1 == i2:
                f = file_info.pop(idx)
                con_message("debug", f"removing {f['name']} from file list")
                break


def monotonic_check(path, idx, bndsname):
    _, name = os.path.split(path)
    with xr.open_dataset(path, decode_times=False) as ds:
        # start at -1 so that the 0th time step doesnt trigger
        try:
            start_bound = ds[bndsname][0].values[0]
            end_bound = ds[bndsname][-1].values[-1]
        except IndexError as e:
            con_message(
                "info", "printing index error"
            )  # only to escape progress-bar prepend
            con_message("error", f"{name} doesnt have expect time_bnds variable shape")
            return None, None, idx
        l1, l2 = -1.0, -1.0
        for bounds in ds[bndsname]:
            b1, b2 = bounds.values
            if (l1 == -1.0 and b1 == 0.0) or (b1 == b2):
                # the daily files have a 0 width time step at the start
                continue
            if b1 > l1 and b2 > l2:
                l1 = b1
                l2 = b2
            else:
                con_message(
                    "error",
                    f"{name} has failed the monotonically-increaseing time bounds check, {(b1, b2)} isn't greater than {(l1, l2)}",
                )
                return None, None, idx

        return start_bound, end_bound, idx


def collect_segments(inpath, num_jobs, timename, bndsname):

    con_message("info", "starting segment collection")
    # collect all the files and sort them by their date stamp
    paths = [os.path.join(inpath, x) for x in os.listdir(inpath) if x.endswith(".nc")]
    for idx, path in enumerate(paths):
        if not os.path.getsize(path):
            _, n = os.path.split(path)
            con_message("warning", f"File {n} is zero bytes, skipping it")
            paths.pop(idx)

    with ProcessPoolExecutor(max_workers=num_jobs) as pool:
        futures = [
            pool.submit(monotonic_check, path, idx, bndsname)
            for idx, path in enumerate(paths)
        ]
        file_info = []
        for future in tqdm(
            as_completed(futures),
            desc="Checking files for monotonically increasing time indices",
            total=len(futures),
        ):
            b1, b2, idx = future.result()
            # if the first value is None, then the file failed its check
            # and the second value is the index of the file that failed
            if not b1:
                # we can simply not add the entry to the file_info list
                pass
            else:
                file_info.append({"name": paths[idx], "start": b1, "end": b2})

    file_info.sort(key=lambda i: i["start"])

    filter_files(file_info)

    # prime the segments by adding the first file
    f1 = file_info.pop(0)
    segments = {(f1["start"], f1["end"]): [f1["name"]]}

    while len(file_info) > 0:

        file = file_info.pop(0)
        joined = False
        for segstart, segend in segments.keys():
            # the start of the file aligns with the end of the segment
            if segend == file["start"]:
                segments[(segstart, file["end"])] = segments.pop((segstart, segend)) + [
                    file["name"]
                ]
                joined = True
                break
            # the end of the file aligns with the start of the segment
            elif segstart == file["end"]:
                segments[(file["start"], segend)] = [file["name"]] + segments.pop(
                    (segstart, segend)
                )
                joined = True
                break
        if not joined:
            if file["start"] == 0.0:
                con_message(
                    "error", f"the file {file['name']} has a start index of 0.0"
                )
                sys.exit(1)
            if segments.get((file["start"], file["end"])):
                con_message(
                    "error",
                    f"the file {file['name']} has perfectly matching time indices with the previous segment {segments.get((file['start'], file['end']))}",
                )
                sys.exit(1)
            segments[(file["start"], file["end"])] = [file["name"]]

    num_segments = len(segments)
    if num_segments > 10:
        con_message(
            "warning",
            f"There were {num_segments} found, this is high. Probably something wrong with the dataset",
        )

    con_message("warning", f"Found {num_segments} segments:")
    for seg in segments.keys():
        con_message("warning", f"Segment {seg} has length {len(segments[seg])}")

    # filter out segments that are completely contained by others
    combos = list(combinations(segments, 2))
    for combo in combos:
        if combo[0][0] > combo[1][0] and combo[0][1] < combo[1][1]:
            segments.pop(combo[0])
        elif combo[1][0] > combo[0][0] and combo[1][1] < combo[0][1]:
            segments.pop(combo[1])
    return segments


def update_history(ds):
    """Add or append history to attributes of a data set"""

    thiscommand = (
        datetime.now().strftime("%a %b %d %H:%M:%S %Y") + ": " + " ".join(sys.argv[:])
    )
    if "history" in ds.attrs:
        newhist = "\n".join([thiscommand, ds.attrs["history"]])
    else:
        newhist = thiscommand
    ds.attrs["history"] = newhist


def write_netcdf(ds, fileName, fillValues=netCDF4.default_fillvals, unlimited=None):
    """Write an xarray Dataset with NetCDF4 fill values where needed"""
    encodingDict = {}
    variableNames = list(ds.data_vars.keys()) + list(ds.coords.keys())
    for variableName in variableNames:
        isNumeric = np.issubdtype(ds[variableName].dtype, np.number)
        if isNumeric:
            dtype = ds[variableName].dtype
            for fillType in fillValues:
                if dtype == np.dtype(fillType):
                    encodingDict[variableName] = {"_FillValue": fillValues[fillType]}
                    break
        else:
            encodingDict[variableName] = {"_FillValue": None}

    update_history(ds)

    if unlimited:
        ds.to_netcdf(fileName, encoding=encodingDict, unlimited_dims=unlimited)
    else:
        ds.to_netcdf(fileName, encoding=encodingDict)


def get_time_units(path):
    with xr.open_dataset(path, decode_times=False) as ds:
        return ds["time"].attrs["units"]


def get_time_names(path):
    with xr.open_dataset(path, decode_times=False) as ds:
        if (tb := ds.get("time_bounds")) and tb.any():
            return "time", "time_bounds"
        else:
            return "time", "time_bnds"


def main():
    desc = """This tool will search through a directory full of raw E3SM model time-slice output files, and find/fix any issues with the time index.
    If overlapping time segments are found, it will find the last file of the preceding segment and truncate it to match the index from the first file from the
    second segment."""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        "input",
        help="The directory to check for time index issues, should only contain a single time-frequency from a single case",
    )
    parser.add_argument(
        "--output",
        default="output",
        required=False,
        help=f"output directory for rectified dataset, default is {os.environ['PWD']}/output",
    )
    parser.add_argument(
        "--move",
        action="store_true",
        required=False,
        help="move the files from the input directory into the output directory instead of symlinks",
    )
    parser.add_argument(
        "--copy",
        action="store_true",
        required=False,
        help="copy the files from the input directory into the output directory instead of symlinks",
    )
    parser.add_argument(
        "-j",
        "--jobs",
        default=8,
        type=int,
        help="the number of processes, default is 8",
    )
    parser.add_argument(
        "--dryrun",
        action="store_true",
        help="Collect the time segments, but dont produce the truncated files or move anything",
    )
    parser.add_argument(
        "--no-gaps", action="store_true", help="Exit if a time gap is discovered"
    )
    parser.add_argument(
        "-q", "--quiet", action="store_true", help="Suppress progress bars"
    )

    args = parser.parse_args()
    inpath = args.input
    outpath = args.output
    num_jobs = args.jobs
    dryrun = args.dryrun

    if args.copy and args.move:
        con_message("error", "Both copy and move flags are set, please only pick one")
        return 1

    if os.path.exists(outpath) and len(os.listdir(outpath)):
        con_message(
            "error", f"Output directory {outpath} already exists and contains files"
        )
        return 1
    else:
        os.makedirs(outpath, exist_ok=True)

    timename, bndsname = get_time_names(next(Path(inpath).glob("*")).as_posix())

    segments = collect_segments(inpath, num_jobs, timename, bndsname)

    if len(segments) == 1:
        con_message("info", "No overlapping segments found")
        if dryrun:
            con_message("info", "not moving files")
        else:
            desc = "Placing files into output directory"
            index, files = segments.popitem()
            for src in tqdm(files, desc=desc):
                _, name = os.path.split(src)
                dst = os.path.join(outpath, name)
                if args.move:
                    move_file(src, dst)
                elif args.copy:
                    copyfile(src, dst)
                else:
                    os.symlink(src, dst)
        return 0

    ordered_segments = []
    for start, end in segments.keys():
        ordered_segments.append(
            {"start": start, "end": end, "files": segments[(start, end)]}
        )

    ordered_segments.sort(key=lambda i: i["start"])

    for s1, s2 in zip(ordered_segments[:-1], ordered_segments[1:]):
        if s2["start"] > s1["end"]:
            # units = get_time_units(s1['files'][0])
            # {units.split(' ')[0]}
            msg = f"There's a time gap between the end of {os.path.basename(s1['files'][-1])} and the start of {os.path.basename(s2['files'][0])} of {s2['start'] - s1['end']} "
            if args.no_gaps:
                outpath = Path(outpath)
                if not any(outpath.iterdir()):
                    outpath.rmdir()
                con_message("error", msg)
                sys.exit(1)
            else:
                con_message("warning", msg)
                if not args.dryrun:
                    con_message("info", "Moving files from the last segment")
                    desc = "Placing files into output directory"
                    for src in tqdm(s1["files"], desc=desc):
                        _, name = os.path.split(src)
                        dst = os.path.join(outpath, name)
                        if args.move:
                            move_file(src, dst)
                        elif args.copy:
                            copyfile(src, dst)
                        else:
                            os.symlink(src, dst)
                continue

        to_truncate = None  # the file that needs to be truncated
        # the index in the file list of segment 1
        truncate_index = len(s1["files"])
        for file in s1["files"][::-1]:
            with xr.open_dataset(file, decode_times=False) as ds:
                if ds[bndsname][-1].values[1] > s2["start"]:
                    truncate_index -= 1
                    continue
                else:
                    break

        con_message(
            "info",
            f"removing {len(s1['files']) - truncate_index} files from ({s1['start']}, {s1['end']})",
        )

        new_ds = xr.Dataset()
        to_truncate = s1["files"][truncate_index]
        with xr.open_dataset(to_truncate, decode_times=False) as ds:
            target_index = 0
            for i in range(0, len(ds[bndsname])):
                if ds[bndsname][i].values[1] == s2["start"]:
                    target_index += 1
                    break
                target_index += 1

            con_message(
                "info",
                f"truncating {to_truncate} by removing {len(ds[bndsname]) - target_index} time steps",
            )

            new_ds.attrs = ds.attrs
            for variable in ds.data_vars:
                if "time" not in ds[variable].coords and timename != "Time":
                    new_ds[variable] = ds[variable]
                    new_ds[variable].attrs = ds[variable].attrs
                    continue
                if timename == "time":
                    new_ds[variable] = ds[variable].isel(time=slice(0, target_index))
                    new_ds[variable].attrs = ds[variable].attrs
                else:
                    new_ds[variable] = ds[variable].isel(Time=slice(0, target_index))
                    new_ds[variable].attrs = ds[variable].attrs

        _, to_truncate_name = os.path.split(to_truncate)
        outfile_path = os.path.join(outpath, f"{to_truncate_name[:-3]}.trunc.nc")

        if dryrun:
            con_message("info", f"dryrun, not writing out file {outfile_path}")
        else:
            con_message("info", f"writing out {outfile_path}")
            write_netcdf(new_ds, outfile_path, unlimited=[timename])

        if dryrun:
            con_message("info", "dryrun, not moving files")
        else:
            desc = "Placing files into output directory"
            con_message("info", f"Moving the first {truncate_index} files")
            for src in tqdm(s1["files"][:truncate_index], desc=desc):
                _, name = os.path.split(src)
                dst = os.path.join(outpath, name)
                if args.move:
                    move_file(src, dst)
                elif args.copy:
                    copyfile(src, dst)
                else:
                    os.symlink(src, dst)
    if dryrun:
        con_message("info", "dryrun, not moving files")
    else:
        con_message("info", "Moving files from the last segment")
        desc = "Placing files into output directory"
        for src in tqdm(ordered_segments[-1]["files"], desc=desc):
            _, name = os.path.split(src)
            dst = os.path.join(outpath, name)
            if args.move:
                move_file(src, dst)
            elif args.copy:
                copyfile(src, dst)
            else:
                os.symlink(src, dst)

    return 0


if __name__ == "__main__":
    sys.exit(main())
