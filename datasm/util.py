import sys
import json
import traceback
import inspect
import logging
import requests
import time
import xarray as xr

from tempfile import NamedTemporaryFile
from subprocess import Popen, PIPE
from pathlib import Path
from datetime import datetime
from pytz import UTC
from termcolor import colored, cprint


def get_UTC_TS():
    return UTC.localize(datetime.utcnow()).strftime("%Y%m%d_%H%M%S_%f")

def get_UTC_YMD():
   return UTC.localize(datetime.utcnow()).strftime("%Y%m%d")


def load_file_lines(file_path):
    if not file_path:
        return list()
    file_path = Path(file_path)
    if not file_path.exists() or not file_path.is_file():
        raise ValueError(
            f"file at path {file_path.resolve()} either doesnt exist or is not a regular file"
        )
    with open(file_path, "r") as instream:
        retlist = [
            [i for i in x.split("\n") if i].pop()
            for x in instream.readlines()
            if x[:-1]
        ]
    return retlist

# -----------------------------------------------

def collision_free_name(apath, abase):
    ''' 
        assuming we must protect a file's extension "filename.ext"
        we test for name.ext, name(1).ext, name(2).ext, ... in apath
        and create from "abase = name.ext" whatever is next in that
        sequence.
    '''
    complist = abase.split('.')
    if len(complist) == 1:
        corename = abase
        ext_name = ""
    else:
        corename = '.'.join(complist[:-1])
        ext_name = '.' + complist[-1]

    abase = ''.join([corename, ext_name])
    dst = os.path.join(apath, abase)
    alt = 0
    ret_file = abase
    while os.path.exists(dst):
        alt += 1
        ret_core = corename + '(' + str(alt) + ')'
        ret_file = ''.join([ret_core, ext_name])
        dst = os.path.join(apath, ret_file)

    return ret_file

# -----------------------------------------------

def get_last_status_line(file_path):
    with open(file_path, "r") as instream:
        last_line = None
        for line in instream.readlines():
            if "STAT" in line:
                last_line = line
        return last_line

# -----------------------------------------------
# unify status case values

def upper_list(alist):
    retlist = list()
    for item in alist:
        if type(item) is str:
            retlist.append(item.upper())
        else:
            print(f"ERROR: item type = {type(item)}")

    return retlist

def upper_dict(adict):
    retdict = dict()
    for akey in adict:
        bkey = akey.upper()
        aval = adict[akey]
        if type(aval) is dict:
            retdict[bkey] = upper_dict(aval)
        elif type(aval) is list:
            retdict[bkey] = upper_list(aval)
        else:
            print(f"ERROR: aval type = {type(aval)}")

    return retdict


# -----------------------------------------------


def print_list(prefix, items):
    for x in items:
        print(f"{prefix}{x}")


# -----------------------------------------------


def print_file_list(outfile, items):
    with open(outfile, "w") as outstream:
        for x in items:
            outstream.write(f"{x}\n")


# -----------------------------------------------


def print_debug(e):
    """
    Print an exceptions relevent information
    """
    print("1", e.__doc__)
    print("2", sys.exc_info())
    print("3", sys.exc_info()[0])
    print("4", sys.exc_info()[1])
    _, _, tb = sys.exc_info()
    print("5", traceback.print_tb(tb))


# -----------------------------------------------


def setup_logging(loglevel, logpath):
    logname = logpath + "-" + get_UTC_TS()
    if loglevel == "debug":
        level = logging.DEBUG
    elif loglevel == "error":
        level = logging.ERROR
    elif loglevel == "warning":
        level = logging.WARNING
    else:
        level = logging.INFO
    logging.basicConfig(
        filename=logname,
        # format="%(asctime)s:%(levelname)s:%(module)s:%(message)s",
        format="%(asctime)s_%(msecs)03d:%(levelname)s:%(message)s",
        datefmt="%Y%m%d_%H%M%S",
        level=level,
    )
    logging.Formatter.converter = time.gmtime
    # should be a separate message call
    # logging.info(f"Starting up the warehouse with parameters: \n{pformat(self.__dict__)}")


# -----------------------------------------------


def con_message(level, message):  # message ONLY to console (in color)

    process_stack = inspect.stack()[1]
    # for item in process_stack:
    #     print(f'DEBUG UTIL: process_stack item = {item}')
    parent_module = inspect.getmodule(process_stack[0])
    # print(f'DEBUG UTIL: (from getmodule(process_stack[0]): parent_module.__name__ = {parent_module.__name__}')
    parent_name = parent_module.__name__.split(".")[-1].upper()
    if parent_name == "__MAIN__":
        parent_name = process_stack[1].split(".")[0].upper()
    message = f"{parent_name}:{message}"

    level = level.upper()
    colors = {"INFO": "white", "WARNING": "yellow", "ERROR": "red", "DEBUG": "cyan"}
    color = colors[level]
    tstamp = get_UTC_TS()
    # to the console
    msg = f"{tstamp}:{level}:{message}"
    cprint(msg, color)


# -----------------------------------------------


def log_message(level, message, user_level='INFO'):  # message BOTH to log file and to console (in color)

    process_stack = inspect.stack()[1]
    parent_module = inspect.getmodule(process_stack[0])
    
    parent_name = parent_module.__name__.split(".")[-1].upper()
    if parent_name == "__MAIN__":
        parent_name = process_stack[1].split(".")[0].upper()
    message = f"{parent_name}:{message}"

    level = level.upper()
    colors = {"INFO": "white", "WARNING": "yellow", "ERROR": "red", "DEBUG": "cyan"}
    color = colors.get(level, 'red')
    tstamp = get_UTC_TS()
    # first, print to logfile
    if level == "DEBUG":
        logging.debug(message)
    elif level == "ERROR":
        logging.error(message)
    elif level == "WARNING":
        logging.warning(message)
    elif level == "INFO":
        logging.info(message)
    else:
        print(f"ERROR: {level} is not a valid log level")

    if level == 'DEBUG' and user_level != level:
        pass
    else:
        # now to the console
        msg = f"{tstamp}:{level}:{message}"
        cprint(msg, color)


# -----------------------------------------------

def json_readfile(filename):
    with open(filename, "r") as file_content:
        json_in = json.load(file_content)
    return json_in

def json_writefile(indata, filename):
    exdata = json.dumps( indata, indent=2, separators=(',\n', ': ') )
    with open(filename, "w") as file_out:
        file_out.write( exdata )

def get_first_nc_file(ds_ver_path):
    dsPath = Path(ds_ver_path)
    for anyfile in dsPath.glob("*.nc"):
        return Path(dsPath, anyfile)

def latest_dspath_version(dspath):
    latest_version = sorted(
        [
            str(x.name)
            for x in dspath.iterdir()
            if x.is_dir() and any(x.iterdir()) and "tmp" not in x.name
        ]
    ).pop()
    return latest_version

def set_version_in_user_metadata(metadata_path, dsversion):     # set version "vYYYYMMDD" in user metadata

    in_data = json_readfile(metadata_path)
    in_data["version"] = dsversion
    json_writefile(in_data,metadata_path)

def get_dataset_version_from_file_metadata(ds_path):
    ds_path = Path(ds_path)
    if not ds_path.exists():
        return 'NONE'
    latest_dir = latest_dspath_version(ds_path)
    first_file_path = ds_path / latest_dir
    first_file = get_first_nc_file(first_file_path)
    if first_file == None:
        return 'NONE'

    ds = xr.open_dataset(first_file)
    if 'version' in ds.attrs.keys():
        ds_version = ds.attrs['version']
    else:
        ds_version = 'NONE'
    return ds_version




# -----------------------------------------------


def search_esgf(
    project,
    facets,
    node="esgf-node.llnl.gov",
    filter_values=[
        "cf_standard_name",
        "variable",
        "variable_long_name",
        "variable_units",
    ],
    latest="true",
):
    """
    Make a search request to an ESGF node and return information about the datasets that match the search parameters

    Parameters:
        project (str): The ESGF project to search inside
        facets (dict): A dict with keys of facets, and values of facet values to search
        node (str): The esgf index node to querry
        filter_values (list): A list of string values to be filtered out of the return document
        latest (str): boolean (true/false not True/False) to search for only the latest version of a dataset
    """
    url = f"https://{node}/esg-search/search/?offset=0&limit=10000&project={project}&format=application%2Fsolr%2Bjson&latest={latest}&{'&'.join([f'{k}={v}' for k,v in facets.items()])}"
    log_message("info", f"search_esgf: issues URL: {url}")
    req = requests.get(url)
    if req.status_code != 200:
        log_message("error", f"util.py: search_esgf: ESGF search request failed: (stat_code {req.status_code}) {url}")
        raise ValueError(f"ESGF search request failed: {url}")

    docs = [
        {k: v for k, v in doc.items() if k not in filter_values}
        for doc in req.json()["response"]["docs"]
    ]
    log_message("info", f"util.py: search_esgf: returning docs len={len(docs)}")
    return docs


# -----------------------------------------------
