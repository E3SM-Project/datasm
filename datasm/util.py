import sys
import os
import subprocess
import re
import shutil
import json
import yaml
import traceback
import inspect
import logging
import requests
import time
import xarray as xr

from tempfile import NamedTemporaryFile
from subprocess import Popen, PIPE
from pathlib import Path
from datetime import datetime, timezone
from termcolor import colored, cprint


def get_UTC_TS():
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

def get_UTC_YMD():
   return datetime.now(timezone.utc).strftime("%Y%m%d")

def dirlist(directory):
    retlist = list()
    try:
        for entry in os.listdir(directory):
            path = os.path.join(directory, entry)
            if os.path.isfile(path):  # Check if it's a regular file
                retlist.append(path)
    except FileNotFoundError:
        print(f"The directory '{directory}' does not exist.")
    except PermissionError:
        print(f"Permission denied to access '{directory}'.")
    retlist.sort()
    return retlist

def dircount(directory):
    return len(dirlist(directory))


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

def fappend(afile: str, amsg: str):
    with open(afile, 'a') as file:
        file.write(amsg + "\n")



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

DSM_ROOT_PATHS = dict()

def get_dsm_paths():
    global DSM_ROOT_PATHS

    if len(DSM_ROOT_PATHS) > 0:
        return DSM_ROOT_PATHS

    gp = os.environ['DSM_GETPATH']
    path_lines = subprocess.run([gp, "ALL"],stdout=subprocess.PIPE,text=True).stdout.strip()
    path_lines = path_lines.split('\n')

    for aline in path_lines:
        path_key, path_val = aline.split(':')
        DSM_ROOT_PATHS[path_key] = path_val

    return DSM_ROOT_PATHS

def ensure_status_file_for_dsid(dsid):
    dsm_paths = get_dsm_paths()
    sf_root1 = dsm_paths['STAGING_STATUS']
    staging = dsm_paths['DSM_STAGING']
    sf_root2 = os.path.join(staging, "status_ext")

    facets = dsid.split('.')
    project = facets[0]
    if project == "CMIP6":
        instid = facets[2];
        if instid == "E3SM-Project" or instid == "UCSB":
            sf_root = sf_root1
        else:
            sf_root = sf_root2

    sf_name = f"{dsid}.status"
    sf_path = os.path.join(sf_root, sf_name)
    if os.path.exists(sf_path):
        return sf_path

    # new status file
    with open(sf_path, 'w') as afile:
        afile.write(f"DATASETID={dsid}")

    return sf_path
    

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
    if loglevel == "debug":
        level = logging.DEBUG
    elif loglevel == "error":
        level = logging.ERROR
    elif loglevel == "warning":
        level = logging.WARNING
    else:
        level = logging.INFO
    logging.basicConfig(
        filename=logpath,
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

    """
    process_stack = inspect.stack()[1]
    parent_module = inspect.getmodule(process_stack[0])
    
    parent_name = parent_module.__name__.split(".")[-1].upper()
    if parent_name == "__MAIN__":
        parent_name = process_stack[1].split(".")[0].upper()
    message = f"{parent_name}:{message}"
    """

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

    """
    if level == 'DEBUG' and user_level != level:
        pass
    else:
        # now to the console
        msg = f"{tstamp}:{level}:{message}"
        cprint(msg, color)
    """


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
    log_message("info", f"Seeking latest vdir in path: {dspath}")
    versions = sorted(
        [
            str(x.name)
            for x in dspath.iterdir()
            if x.is_dir() and any(x.iterdir()) and "tmp" not in x.name
        ]
    )
    if len(versions):
        latest_version = versions.pop()
        return latest_version
    return None

def get_dataset_version_from_file_metadata(latest_dir):  # input latest_dir already includes version leaf directory
    ds_path = Path(latest_dir)
    if not ds_path.exists():
        log_message("info", f"No version: no path {latest_dir}")
        return 'NONE'
    
    first_file = get_first_nc_file(latest_dir)
    if first_file == None:
        log_message("info", f"No first_file")
        return 'NONE'

    log_message("error", f"(Fake_Error) Testing for version in file {first_file}")

    ds = xr.open_dataset(first_file)
    if 'version' in ds.attrs.keys():
        ds_version = ds.attrs['version']
        if not re.match(r"v\d", ds_version):
            log_message("info", f"Invalid version {ds_version} in metadata")
            ds_version = 'NONE'
    else:
        log_message("info", f"No version in ds.attrs.keys()")
        ds_version = 'NONE'
    return ds_version

def set_version_in_user_metadata(metadata_path, dsversion):     # set version "vYYYYMMDD" in user metadata

    log_message("info", f"set_version_in_user_metadata: path={metadata_path}")
    in_data = json_readfile(metadata_path)
    in_data["version"] = dsversion
    json_writefile(in_data,metadata_path)

def prepare_cmip_job_metadata(cmip_dsid, in_meta_path, slurm_out):

    _, _, institution, model_version, experiment, variant, table, cmip_var, _ = cmip_dsid.split('.')

    metadata_name = f"{experiment}_{variant}.json"

    # more cruft to support v1_LE and v2_LE
    metadata_version = model_version        # metadata_version = CMIP6 "Source" unless v1_LE or v2_LE
    if institution == "UCSB":
        metadata_version = "E3SM-1-0-LE"
    elif model_version == "E3SM-2-0":
        if experiment == "historical":
            rdex = int(variant.split('i')[0].split('r')[1])
            if rdex >= 6 and rdex <= 21:
                metadata_version = "E3SM-2-0-LE"
        elif experiment == "ssp370":
            metadata_version = "E3SM-2-0-LE"

    # copy metadata file to slurm directory for edit
    metadata_path_src = os.path.join(in_meta_path, metadata_version, f"{metadata_name}")
    shutil.copy(metadata_path_src, slurm_out)
    metadata_path =  os.path.realpath(os.path.join(slurm_out, metadata_name))

    # force dataset output version here
    ds_version = "v" + get_UTC_YMD()
    set_version_in_user_metadata(metadata_path, ds_version)
    log_message("debug", f"Set metadata dataset version in {metadata_path} to {ds_version}")

    return metadata_path

def dc_spec_selection(alist,spec):
    # print(f"DEBUG - Selection for : {spec}")
    speclist = spec.split(',')

    for spec_val in speclist:
        # print(f"SPEC_VAL: {spec_val}")
        newlist = list()
        for aline in alist:
            if aline.split(',')[0] == spec_val:
                rlist = (',').join(aline.split(',')[1:])
                newlist.append(rlist)

        alist = newlist

    return alist

def derivative_conf(target_dsid,resource_path):
    
    project = target_dsid.split('.')[0]

    if project == "CMIP6":
        e3sm_dsid = parent_native_dsid(target_dsid)
    else:
        e3sm_dsid = target_dsid

    log_message("info", f"DBG: deriv_conf: target {target_dsid}, parent {e3sm_dsid}")

    project, model, exper, resol, realm, grid, out_type, freq, ensem = e3sm_dsid.split('.')

    # create the dc_spec_selection spec

    selspec = f"{realm},{model},{resol}"
    log_message("info", f"DBG: deriv_conf: generated dc_spec_selection spec: {selspec} for resource_path {resource_path}/derivatives.conf")
    
    # load the derivatives configuration

    dc_file = os.path.join(resource_path, "derivatives.conf")
    dclines = load_file_lines(dc_file)

    spec_1 = f"{selspec},REGRID"
    regrid = dc_spec_selection(dclines,spec_1)
    log_message("info", f"DBG: deriv_conf: obtained {len(regrid)} regrid matches: {regrid}")
    if len(regrid) != 1:
        regrid = "None"
    else:
        regrid = regrid[0]

    spec_2 = f"{selspec},MASK"
    region_mask = dc_spec_selection(dclines,spec_2)
    if len(region_mask) != 1:
        region_mask = "None"
    else:
        region_mask = region_mask[0]

    spec_3 = f"{selspec},FILE_SELECTOR"
    file_selector = dc_spec_selection(dclines,spec_3)
    if len(file_selector) != 1:
        file_selector = "None"
    else:
        file_selector = file_selector[0]

    spec_4 = f"{selspec},CASE_FINDER"
    case_finder = dc_spec_selection(dclines,spec_4)
    if len(case_finder) != 1:
        case_finder = "None"
    else:
        case_finder = case_finder[0]

    ''' produce dictionary of return values '''
    retval = dict()
    retval['hrz_atm_map_path'] = os.path.join(resource_path,'maps',regrid)
    retval['mapfile'] = os.path.join(resource_path,'maps',regrid)
    retval['region_file'] = os.path.join(resource_path,'maps',region_mask)
    retval['file_pattern'] = file_selector
    retval['case_finder'] = case_finder

    return retval


# -----------------------------------------------

def get_e2c_info(cmip_var, freq, realm, data_path, cmip_out, metadata_path, cmip_tables_path):
    workdir = f"{os.getcwd()}"
    log_message("debug",f"get_e2c_info: PWD = {workdir}")

    info_file = NamedTemporaryFile(delete=False)
    log_message("info", f"Obtained temp info file name: {info_file.name}")
    cmd = f"e3sm_to_cmip --info --map none -i {data_path} -o {cmip_out} -u {metadata_path} --freq {freq} -v {cmip_var} -t {cmip_tables_path} --info-out {info_file.name} --realm {realm}"
    log_message("debug", f"{__name__}:get_e2c_info: issuing variable info cmd: {cmd}")

    proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
    _, err = proc.communicate()
    if err:     # anything on stderr, may not be important
        log_message("info", f"(stderr) checking variables: {err}")

    with open(info_file.name, 'r') as instream:
        variable_info = yaml.load(instream, Loader=yaml.SafeLoader)

    if variable_info == None or len(variable_info) == 0:
        log_message("error", f"REAL_ERROR checking variables: No data returned from e3sm_to_cmip --info: {cmd}")
        os._exit(1)

    if isinstance(variable_info, list):
        variable_info = variable_info[0]    # expect a single dictionary

    log_message("debug", f"get_e2c_info: type(variable_info from yaml_loader) = {type(variable_info)}")

    var_info = dict()
    var_info['mlev'] = False
    var_info['plev'] = False
    var_info['natv_vars'] = list()
    var_info['cmip_vars'] = list()
    var_info['natv_plev_vars'] = list()
    var_info['cmip_plev_vars'] = list()


    native_info = list()
    for item in variable_info['E3SM Variables'].split(','):
        native_info.append( item.strip() )

    if 'Levels' in variable_info.keys() and variable_info['Levels']['name'] == 'plev19':
        var_info['plev'] = True
        var_info['natv_plev_vars'].extend(native_info)
        var_info['cmip_plev_vars'].append(variable_info['CMIP6 Name'])
    else:
        var_info['mlev'] = True
        log_message("debug", f"get_e2c_info obtained native_info = {native_info}")
        var_info['natv_vars'].extend(native_info)
        var_info['cmip_vars'].append(variable_info['CMIP6 Name'])

    if not var_info['mlev'] and not var_info['plev']:
        log_message("error", "resolve_cmd: e3sm_to_cmip --info returned EMPTY variable info")
        self._cmd = "echo EMPTY variable info; exit 1"
        os._exit(1)

    return var_info

# -----------------------------------------------

# These (HACK) substitution manipulations should be defined in an external table, not code

def set_e3sm_model_resolution_ensemble(sourceid,institution,experiment,variant):

    src_model = sourceid[5:].replace('-', '_')        # CMIP6 dsids will NOT have "-LE" in the Source_ID
    rdex = int(variant.split('i')[0].split('r')[1])   # ensemble number  
    ens = f"ens{rdex}"

    # HACK for NARRM - only options for now
    if src_model == "2_0":
        resol = "LR"
    elif src_model == "2_0_NARRM":
        resol = "LR-NARRM"
    elif src_model == "2_1":
        resol = "LR"
    else:
        resol = "1deg_atm_60-30km_ocean"

    ret_model = src_model
    # HACK for v1_Large_Ensemble (External)
    if src_model == "1_0" and institution == "UCSB":
        ret_model = "1_0_LE"
    # HACK for v2_Large_Ensemble (External)
    if src_model == "2_0":
        if (rdex >= 6 and rdex <= 21) or experiment == "ssp370":
            ret_model = "2_0_LE"

    return ret_model, resol, ens


def parent_native_dsid(target_dsid):

    project = target_dsid.split('.')[0]

    if project == "E3SM":       # for climo and timeseries, e.g. E3SM.2_0.amip.LR.atmos.180x360.climo.mon.ens1
        # log_message("info", f"parent_native_dsid: received target dsid {target_dsid}")
        project, model, exper, resol, realm, grid, out_type, freq, ensem = target_dsid.split('.')
        if out_type not in [ "climo", "time-series" ] and grid == "native":
            return "None"

        native_dsid = ('.').join([ project, model, exper, resol, realm, "native", "model-output", freq, ensem ])
        return native_dsid

    # Project not E3SM 
    allowed_institutions = [ "E3SM-Project", "UCSB" ]

    project, activ, inst, source, cmip_exp, variant, cmip_realm, _, _ = target_dsid.split('.')
    if project != "CMIP6":
        return "NONE"
    if inst not in allowed_institutions:
        return "NONE"

    model, resol, ens = set_e3sm_model_resolution_ensemble(source,inst,cmip_exp,variant)

    grid = "native"
    otype = "model-output"

    if cmip_realm == "SImon":
        realm = "sea-ice"
    elif cmip_realm in [ '3hr', 'AERmon', 'Amon', 'CFmon', 'day', 'fx' ]:
        realm = "atmos"
    elif cmip_realm in [ 'LImon', 'Lmon' ]:
        realm = "land"
    elif cmip_realm in [ 'Ofx', 'Omon' ]:
        realm = "ocean"
    else:
        return "NONE"

    if cmip_realm in [ 'AERmon', 'Amon', 'CFmon', 'LImon', 'Lmon', 'Omon', 'SImon', 'fx', 'Ofx' ]:
        freq = "mon"
    else:
        freq = cmip_realm

    if model[0:3] == "1_1":
        if cmip_exp == "hist-bgc":
            experiment = "hist-BDRC"
        elif cmip_exp == "historical":
            experiment = "hist-BDRD"
        elif cmip_exp == "ssp585-bgc":
            experiment = "ssp585-BDRC"
        elif cmip_exp == "ssp585":
            experiment = "ssp585-BDRD"
    else:
        experiment = cmip_exp

    native_dsid = ('.').join([ "E3SM", model, experiment, resol, realm, grid, otype, freq, ens ])

    log_message("info", f"DEBUG_TEST: parent_native_dsid: returns native dsid {native_dsid}")

    return native_dsid

def is_vdir_pattern(str):
    return len(str) > 1 and str[0] == 'v' and str[1:2].isdigit() and str[1:].replace('.','',1).isdigit()


def latest_data_vdir(dsid):

    corepath = dsid.replace('.', '/')

    rootpaths = get_dsm_paths()
    enspath1 = os.path.join( rootpaths['STAGING_DATA'], corepath )
    enspath2 = os.path.join( rootpaths['PUBLICATION_DATA'], corepath )

    if os.path.exists(enspath1):
        if os.path.exists(enspath2):
            the_enspath = "BOTH"
        else:
            the_enspath = enspath1
    elif os.path.exists(enspath2):
        the_enspath = enspath2
    else:
        return "NONE"

    # print(f"TEST_DEBUG: STAGE_1: the_enspath = {the_enspath}")

    if the_enspath != "BOTH":
        vdirs = [ f.path for f in os.scandir(the_enspath) if f.is_dir() ]
        if len(vdirs) == 0:
            return "NONE"
        vdirs = sorted(vdirs)
        latest_vdir = vdirs[-1]
        # if populated, return.  Else NONE
        vcount = len([item for item in os.listdir(latest_vdir) if os.path.isfile(os.path.join(latest_vdir, item))])
        if vcount > 0:
            return latest_vdir
        return "NONE"

    # both ens dirs exist

    vdirs1 = [ f.path for f in os.scandir(enspath1) if f.is_dir() ]
    vdirs2 = [ f.path for f in os.scandir(enspath2) if f.is_dir() ]
    if len(vdirs1) == 0:
        if len(vdirs2) == 0:
            return "NONE"
        the_vdirs = sorted(vdirs2)
    elif len(vdirs2) == 0:
        the_vdirs = sorted(vdirs1)
    else:
        the_vdirs = "BOTH"

    # print(f"TEST_DEBUG: STAGE_2: the_vdirs = {the_vdirs}")

    if the_vdirs != "BOTH":
        latest_vdir = the_vdirs[-1]
        # if populated, return.  Else NONE
        vcount = len([item for item in os.listdir(latest_vdir) if os.path.isfile(os.path.join(latest_vdir, item))])
        if vcount > 0:
            return latest_vdir
        return "NONE"

    # shoot.  BOTH ensdirs have vdirs.  If only the latest of one is populated, use it
    # otherwise, we select the vdir with latest (highest sorted) vdir tail name.
    latest_vdir1 = vdirs1[-1]
    latest_vdir2 = vdirs2[-1]

    vcount1 = len([item for item in os.listdir(latest_vdir1) if os.path.isfile(os.path.join(latest_vdir1, item))])
    vcount2 = len([item for item in os.listdir(latest_vdir2) if os.path.isfile(os.path.join(latest_vdir2, item))])

    if vcount1 == 0:
        if vcount2 == 0:
            return "NONE"
        return latest_vdir2
    elif vcount2 == 0:
        return latest_vdir1

    # compare the tails
    vtail1 = latest_vdir1.split('/')[-1]
    vtail2 = latest_vdir2.split('/')[-1]

    if vtail1 > vtail2:
        return latest_vdir1
    return latest_vdir2


def latest_aux_data(in_dsid, atype, for_e2c):

    project = in_dsid.split('.')[0]
    if project == "CMIP6":
        e3sm_dsid = parent_native_dsid(in_dsid)
        if e3sm_dsid == "NONE":
            return "NONE"
    else:
        e3sm_dsid = in_dsid

    _, model, exper, resol, realm, grid, otype, freq, ens = e3sm_dsid.split('.')

    if realm == "sea-ice" and atype == "restart" and for_e2c:
        realm = "ocean"

    target_dsid = ('.').join([ "E3SM", model, exper, resol, realm, grid, atype, "fixed", ens ])

    # print(f"DEBUG_TEST: target_dsid = {target_dsid}")

    latest_dir = latest_data_vdir(target_dsid)
    if latest_dir == "NONE":
        return "NONE"

    # get full path to the first file from this directory
    the_file = os.listdir(latest_dir)[0]

    # print(f"THE FILE: {the_file}")
    return os.path.join(latest_dir, the_file)


