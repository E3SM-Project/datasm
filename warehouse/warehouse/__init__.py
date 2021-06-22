import os
import sys
import argparse
import time
from pathlib import Path
from argparse import RawTextHelpFormatter
from datetime import datetime

from warehouse.dataset import Dataset
from warehouse.util import print_file_list, load_file_lines, print_list


def specialize_expname(experiment, reso, tune):
    if experiment == "F2010plus4k":
        experiment = "F2010-plus4k"
    if experiment[:5] == "F2010" or experiment == "1950-Control":
        if reso[:4] == "1deg" and tune == "highres":
            experiment = experiment + "-LRtunedHR"
        else:
            experiment = experiment + "-HR"
    return experiment


def get_dsid_arch_key(dsid):
    """
    Return a tuple of (model version, experiment name, ensemble number)
    """
    comps = dsid.split(".")
    expname = specialize_expname(comps[2], comps[3], comps[4])
    return comps[1], expname, comps[-1]


def get_dsid_type_key(dsid):
    comps = dsid.split(".")
    realm = comps[-5]
    gridv = comps[-4]
    otype = comps[-3]
    freq = comps[-2]

    if realm == "atmos":
        realm = "atm"
    elif realm == "land":
        realm = "lnd"
    elif realm == "ocean":
        realm = "ocn"

    if gridv == "native":
        grid = "nat"
    elif otype == "climo":
        grid = "climo"
    elif otype == "monClim":
        grid = "climo"
        freq = "mon"
    elif otype == "seasonClim":
        grid = "climo"
        freq = "season"
    elif otype == "time-series":
        grid = "reg"
        freq = "ts-" + freq
    elif gridv == "namefile":
        grid = "namefile"
        freq = "fixed"
    elif gridv == "restart":
        grid = "restart"
        freq = "fixed"
    else:
        grid = "reg"
    return "_".join([realm, grid, freq])


def path_to_id(path):
    """
    Turn a path into a dataset id
    # dsid:  root,model,experiment.resolution. ... .realm.grid.otype.ens.vcode
    """
    return ".".join(path.split(os.sep)[5:])


def get_version_dirs(rootpath, mode):
    """
    Find the version directory paths relative to the root. Allowed mode values are:
        all - find all versions
        empty - find version directories with no files
        nonempty - find version directories that have files
    """
    allowed_modes = ["all", "empty", "nonempty"]
    if mode not in allowed_modes:
        raise ValueError(f"mode {mode} not in set of allowed modes {allowed_modes}")

    selected = []
    for root, dirs, files in os.walk(rootpath):
        # if its a leaf directory, then it should be the version directory
        if not dirs:
            selected.append(root)

    if mode == "all":
        return selected
    elif mode == "empty":
        empty = []
        for item in selected:
            for root, dirs, files in os.walk(item):
                if not files:
                    empty.append(item)
        return empty
    elif mode == "nonempty":
        nonempty = []
        for item in selected:
            for root, dirs, files in os.walk(item):
                if files:
                    nonempty.append(item)
        return nonempty
    else:
        raise ValueError(f"Not recognized mode: {mode}, shouldnt ever get here")


def get_ensemble_dirs(warehouse_root, print_paths=False, paths_out=None):
    """
    get ALL warehouse ensemble directories
    """
    version_dirs = get_version_dirs(warehouse_root, "all")
    if print_paths:
        print_file_list(paths_out, version_dirs)

    ensemble_paths = []
    for adir in version_dirs:
        ensdir, vdir = os.path.split(adir)
        ensemble_paths.append(ensdir)

    ensemble_paths = sorted(list(set(ensemble_paths)))
    return ensemble_paths


# from list [ (timestamp,'category:subcat:subcat:...') ] tuples
# return dict { category: [ (timestamp,'subcat:subcat:...') ], ... tuple-lists
# may be called on dict[category] for recursive breakdown of categories, all tuple lists sorted on their original timestamps
def load_ds_status_list(ensdirs):
    """"""
    wh_status = {}
    for edir in ensdirs:
        dsid = path_to_id(edir)
        akey = get_dsid_arch_key(dsid)
        dkey = get_dsid_type_key(dsid)
        if not akey in wh_status.keys():
            wh_status[akey] = {}
        wh_status[akey][dkey] = Dataset(path=edir)
    return wh_status


def produce_status_listing_vcounts(datasets):

    statlinelist = []
    for akey in datasets.keys():
        for dkey in datasets[akey].keys():
            ds = datasets[akey][dkey]
            if not ds.path.exists():
                continue
            # ds.versions = { 'v0': fcount , 'v1': fcount, ... }
            statlist = ["_" * 10] * 6
            for idx, v in enumerate(sorted(list(ds.versions))):
                statlist[idx] = f"{v}:[{ds.versions[v]}]".ljust(10, "_")

            statbar = ".".join(statlist)
            # make something like '1_0,1950-Control-21yrContHiVol-HR,ens1,atm_nat_3hr'
            ds_spec = f'{",".join(akey)},{dkey}'

            if len(ds.stat["WAREHOUSE"]):
                timestamp, val = ds.get_latest()
                stat_general = f"{timestamp}:{val}"
            else:
                stat_general = "NO_STATFILE"

            statline = f"{ds_spec:60}|{stat_general:40}|{statbar}|{ds.path}"
            statlinelist.append(statline)

    return sorted(list(set(statlinelist)))


def parse_args(arg_sources, checkers):
    DESC = "Automated E3SM data warehouse utilities"
    parser = argparse.ArgumentParser(
        prog="warehouse",
        description=DESC,
        prefix_chars="-",
        formatter_class=RawTextHelpFormatter,
    )

    subcommands = parser.add_subparsers(
        title="subcommands", description="warehouse subcommands", dest="subparser_name"
    )

    subparsers = {}
    for source in arg_sources:
        name, sub = source(subcommands)
        subparsers[name] = sub

    parsed_args = parser.parse_args()

    valid, name = checkers[parsed_args.subparser_name](parsed_args)
    if not valid:
        print("invalid")
        subparsers[name].print_help()
        return None

    return parsed_args
