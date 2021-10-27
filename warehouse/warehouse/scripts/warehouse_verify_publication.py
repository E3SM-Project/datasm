import sys, os
import json
import argparse
from argparse import RawTextHelpFormatter

import traceback
import inspect
import logging
import requests
import time

from tempfile import NamedTemporaryFile
from subprocess import Popen, PIPE
from pathlib import Path
from datetime import datetime
from pytz import UTC
from termcolor import colored, cprint


# -----------------------------------------------

def ts():
    return 'TS_' + pytz.utc.localize(datetime.utcnow()).strftime("%Y%m%d_%H%M%S_%f")


helptext = '''
    Requires input file of one or more dataset_ids.

    The default behavior is to verify the ESGF publication status (proper match of files) for each dataset_id whose
    data is found in the (default) publication_root directories, and reflect the status results to the console.

    In most cases, the line "<dataset_id>:<status>" is printed to stdout for each given dataset_id.

    If -u, --update-status is supplied, then the corresponding dataset status file is updated accordingly.

    If no status file can be found, one is NOT created. (May want to change that with a "--force-status" flag.)

    For each dataset_id listed in the input file:
        If the dataset is published and the pub_root version matches the esgf latest version and the list of files match,
            Then report "PUBLICATION:Verified" (appending appropriate state to the status file if "-u" supplied.)
        If the dataset is pub_root but appears unpublished, or other elements do not match,
            Then report "PUBLICATION:Verification_Fail:<reasons>" to the status file.
        If the dataset is NOT in pub_root, issue warnings but do not update the status file, irrespective of "-u".
'''

gv_stat_root = '/p/user_pub/e3sm/staging/status'
gv_pub_root = '/p/user_pub/work'

def assess_args():

    parser = argparse.ArgumentParser(description=helptext, prefix_chars='-', formatter_class=RawTextHelpFormatter)
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')
    required.add_argument('-i', '--input', action='store', dest="thedsidlist", type=str, required=True)
    optional.add_argument('-u', '--update-status', action='store_true', dest="updatestatus", required=False)

    args = parser.parse_args()

    return args



def setup_logging(loglevel, logpath):
    logname = logpath + "-" + UTC.localize(datetime.utcnow()).strftime("%Y%m%d_%H%M%S_%f")
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
    tstamp = UTC.localize(datetime.utcnow()).strftime("%Y%m%d_%H%M%S_%f")  # for console output
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

def load_file_lines(file_path):
    if not file_path:
        return list()
    file_path = Path(file_path)
    if not file_path.exists() or not file_path.is_file():
        print(f"path {file_path.resolve()} either doesnt exist or is not a regular file")
        return list()
    with open(file_path, "r") as instream:
        retlist = [
            [i for i in x.split("\n") if i].pop()
            for x in instream.readlines()
            if x[:-1]
        ]
    return retlist


# -----------------------------------------------

def raw_search_esgf(
    facets,
    offset="0",
    limit="50",
    node="esgf-node.llnl.gov",
    qtype="Dataset",
    fields="*",
    latest="true",
):
    """
    Make a search request to an ESGF node and return information about the datasets that match the search parameters

    Parameters:
        facets (dict): A dict with keys of facets, and values of facet values to search
        offset (str) : offset into available results to return data
        limit (str)  : number of results to return (default = 50, max = 10000)
        node (str)   : The esgf index node to query
        qtype (str)  : The query type, one of "Dataset" (default), "File" or "Aggregate"
        fields (str) : a comma-separated string of metadata field names, default '*' MUST be overridden.
        latest (str) : boolean (true/false not True/False) to search for only the latest version of a dataset
    """

    if fields == "*":
        print("ERROR: Must specify string of one or more CSV fieldnames with fields=string")
        return None

    if len(facets):
        url = f"https://{node}/esg-search/search/?offset={offset}&limit={limit}&type={qtype}&format=application%2Fsolr%2Bjson&latest={latest}&fields={fields}&{'&'.join([f'{k}={v}' for k,v in facets.items()])}"
    else:
        url = f"https://{node}/esg-search/search/?offset={offset}&limit={limit}&type={qtype}&format=application%2Fsolr%2Bjson&latest={latest}&fields={fields}"

    # print(f"Executing URL: {url}")
    req = requests.get(url)
    # print(f"type req = {type(req)}")

    if req.status_code != 200:
        # print(f"ERROR: ESGF search request returned non-200 status code: {req.status_code}")
        return list()

    docs = [
        {k: v for k, v in doc.items()}
        for doc in req.json()["response"]["docs"]
    ]

    return docs

# ----------------------------------------------

def safe_search_esgf(
    facets,
    node="esgf-node.llnl.gov",
    qtype="Dataset",
    fields="*",
    latest="true",
):
    """
    Make a search request to an ESGF node and return information about the datasets that match the search parameters

    Parameters:
        project (str): The ESGF project to search inside
        facets (dict): A dict with keys of facets, and values of facet values to search
        node (str)   : The esgf index node to query
        qtype (str)  : The query type, one of "Dataset" (default), "File" or "Aggregate"
        fields (str) : a comma-separated string of metadata field names, default '*' MUST be overridden.
        latest (str) : boolean (true/false not True/False) to search for only the latest version of a dataset
    """

    full_docs = list()

    qlimit = 10000
    curr_offset = 0

    while True:
        docs = raw_search_esgf(facets, offset=curr_offset, limit=f"{qlimit}", qtype=f"{qtype}", fields=f"{fields}", latest=f"{latest}")
        # print(f"DEBUG: raw_search returned {len(docs)} records")
        full_docs = full_docs + docs
        if len(docs) < qlimit:
            return full_docs
        curr_offset += qlimit
        # time.sleep(1)

    return full_docs

# ----------------------------------------------

def is_int(str):
    try:
        int(str)
    except:
        return False
    return True

def maxversion(vlist):
    nlist = list()
    for txt in vlist:
        if not is_int(txt[1:]):
            continue
        nlist.append(int(txt[1:]))
    if not nlist or len(nlist) == 0:
        return "vNONE"
    nlist.sort()
    return f"v{nlist[-1]}"

def get_maxv_info(edir):
    ''' given an ensemble directory, return the max "v#" subdirectory and its file count '''
    v_dict = dict()
    for root, dirs, files in os.walk(edir):
        if not dirs:
            v_dict[os.path.split(root)[1]] = len(files)
    maxv = maxversion(v_dict.keys())
    return maxv, v_dict[maxv]

def get_path_files(vdir):
    ''' given a version directory, return the list of ".nc" files contained '''
    for root, dirs, files in os.walk(vdir):
        return files

def get_last_status_value(statlist):
    newlist = list()
    for item in statlist:
        if item.split(':')[0] != "STAT":
            continue
        newlist.append(item.split(':')[2:])
    if len(newlist):
        newlist.sort()
        return newlist[-1]
    return list()

def set_last_status_value(statfile,status_str):
    with open(statfile, "a") as outstream:
        tstamp =  UTC.localize(datetime.utcnow()).strftime("%Y%m%d_%H%M%S_%f")
        msg = f'STAT:{tstamp}:{status_str}'
        outstream.write(msg + "\n")

# -----------------------------------------------

def main():

    pargs = assess_args()
    do_stats = pargs.updatestatus

    dsid_list = load_file_lines(pargs.thedsidlist)

    # print(f"Found {len(dsid_list)} dataset ids.  do_stats = {do_stats}")

    stat_root = Path(gv_stat_root)
    pub_root = Path(gv_pub_root)
    
    for dsid in dsid_list:

        # establish project
        project = dsid.split(".")[0]
        if project == "E3SM":
            project = project.lower()
        add_facet = dict()
        if project == "CMIP6":
            add_facet = { "institution_id": "E3SM-Project" }

        if do_stats:
            statname = f"{dsid}.status"
            statfile = stat_root / statname
            # print(f"statfile = {statfile}")
            statents = load_file_lines(statfile)
            if not statents:
                log_message("error", f"warehouse_verify_publication: No status file entries in file: {statfile}")
                continue
            last_stats = get_last_status_value(statents)
            # print(f"Last Stat = {last_stats}")
        facet_path = Path(dsid.replace('.', '/'))
        ds_path = pub_root / facet_path
        dsp = f"{ds_path}"
        if not os.path.exists(dsp):
            log_message("error", f"warehouse_verify_publication: No dataset publication path exists {dsp}")
            continue
        v_list = next(os.walk(dsp))[1]
        maxv = maxversion(v_list)
        pub_path = ds_path / Path(maxv)
        # print(f"pub_path = {pub_path}")
        pfiles = get_path_files(pub_path)
        if not pfiles or len(pfiles) == 0:
            reason = f"No pub_root publication found for {dsid}"
            statmsg = f"PUBLICATION:Verification_Fail:{reason}"
            print(f"{dsid}:{statmsg}")
            continue
        p_set = set(pfiles)
        p_len = len(p_set)
        # print(f"DBG: p_len = {p_len}")

        # obtain data from ESGF search API
        facets = {"project": f"{project}", "master_id": dsid}
        facets.update(add_facet)

        docs = safe_search_esgf(facets, qtype="Dataset", fields="version,data_node")
        if not docs:
            reason = f"Dataset query returned empty docs"
            log_message("error", f"dataset_id {dsid}: {reason}")
            continue

        for item in docs: # should be only one
            version = item['version']
            data_node = item['data_node']

        dataset_id = f"{dsid}.v{version}|{data_node}"

        facets = {"project": f"{project}", "dataset_id": dataset_id}
        facets.update(add_facet)

        docs = safe_search_esgf({"project": f"{project}", "dataset_id": dataset_id}, qtype="File", fields="title")
        if not docs:
            print(f"{dsid}:Empty docs!")
            continue

        s_set = set({ f"{item['title']}" for item in docs })
        s_len = len(s_set)

        if s_len > 0 and s_set == p_set:   # publication verified
            statmsg = "PUBLICATION:Verified"
        else:
            if( not s_set or s_len == 0 ):
                reason = f"No ESGF publication found for {dsid}"
            else:
                if s_len != p_len:
                    reason = f"MismatchedFilecount:(pubroot;esgf)=({p_len};{s_len})"
                else:
                    reason = "MismatchedFileLists"               
            statmsg = f"PUBLICATION:Verification_Fail:{reason}"

        if do_stats:
            # print(f"(ersatz) stat message: {statmsg}")
            set_last_status_value(statfile, statmsg)

        print(f"{dsid}:{statmsg}")


    sys.exit(0)
        

if __name__ == "__main__":
  sys.exit(main())

