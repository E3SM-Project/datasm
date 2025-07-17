#!/usr/bin/env python3

# Import necessary modules
import sys
import os
import glob
import shutil
import argparse
import yaml
import fnmatch
import subprocess
from argparse import RawTextHelpFormatter
from collections import deque
from datasm.util import setup_logging
from datasm.util import log_message
from datasm.util import dirlist
from datasm.util import dircount
from datasm.util import get_dsm_paths
from datasm.util import load_file_lines
from datasm.util import write_file_list
from datasm.util import fappend
from datasm.util import dsid_to_dict
from datasm.util import parent_native_dsid
from datasm.util import latest_data_vdir
from datasm.util import get_first_nc_file
from datasm.util import ensure_status_file_for_dsid
from datasm.util import get_UTC_TS

helptext = '''
    Usage: dsm_man_cmip --runmode <runmode> -i <file_of_cmip6_dataset_ids> [--ds_spec alternate_dataset_spec.yaml]
           <runmode> must be either "TEST" or "WORK".
           In TEST mode, only the first year of data will be processed,
           and the E3SM dataset status files are not updated."
           In WORK mode, all years given in the dataset_spec are applied,
           and the E3SM dataset status files are updated, and the cmorized
           results are moved to staging data (the warehouse).

    Note: The runtime environment must include the following items:

        1. (suggestion) Use conda/mamba create env -n <name> -f datasm/conda-env/prod.yaml
           to create the runtime environment.
        2. pip install datasm (ensures that datasm.utils are available along with configs)

        3. Place the line
               export DSM_GETPATH=<path_to_DSM_STAGING>/Relocation/.dsm_get_root_path.sh
           into your .bashrc file, so that configs, mapfiles, metadata can be located.
           (on Chrysalis, DSM_STAGING = /lcrc/group/e3sm2/DSM/Staging)

        4. pip install e3sm_to_cmip (or ensure it exists in your environment).

        5. pip install zstash (or ensure it exists in your environment).

        6. The NCO tools "ncclimo" and "ncremap" must exist in the environment.

'''

def assess_args():

    parser = argparse.ArgumentParser(description=helptext, prefix_chars='-', formatter_class=RawTextHelpFormatter)
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')
    required.add_argument('-i', '--input_dsid_list', action='store', dest="input_dsids", type=str, help="file list of CMIP dataset_ids", required=True)
    required.add_argument('--runmode', action='store', dest="run_mode", type=str, help="\"TEST\" or \"WORK\"", required=True)
    optional.add_argument('--ds_spec', action='store', dest="alt_ds_spec", type=str, help="alternative dataset spec", required=False)

    args = parser.parse_args()

    return args


# -----------------------------------------------------------
# Simple Utilities for Code Readability
# -----------------------------------------------------------

def linex(path: str, keyv: str, delim: chr, pos: int):
    if not os.path.exists(path):
        print(f"ERROR: linex: no path {path}")
        return ""
    with open(path, 'r') as thefile:
        for aline in thefile:
            if keyv in aline:
                if delim == "":
                    return aline.rstrip()
                else:
                    return aline.split(delim)[pos].rstrip()
    return ""

def lines_match(path: str, keyv: str, delim: chr, pos: int):
    retlist = list()
    if not os.path.exists(path):
        print(f"ERROR: linex: no path {path}")
        return retlist
    with open(path, 'r') as thefile:
        for aline in thefile:
            aline = aline.rstrip()
            if keyv in aline:
                if delim == "":
                    retlist.append(aline)
                elif aline.split(delim)[pos] == keyv:
                    retlist.append(aline)
    return retlist

def quiet_remove(afile):
    if os.path.isfile(afile):
        os.remove(afile)

def glob_move(src_pattern, dest_dir):
    os.makedirs(dest_dir, exist_ok=True)

    # Use glob to find all files matching the pattern
    files_to_move = glob.glob(src_pattern)

    errors = 0
    # Move each file to the destination directory
    for file_path in files_to_move:
        try:
            shutil.move(file_path, dest_dir)
        except Exception as e:
            print(f"An error occurred while moving '{file_path}': {e}")
            errors += 1
    if errors > 0:
        return False
    return True

def get_directory_content_size(directory):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(directory):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            # Add the size of each file to the total size
            total_size += os.path.getsize(filepath)
    return total_size

dsm_paths = get_dsm_paths()
archive_manage = dsm_paths['ARCHIVE_MANAGEMENT']
archive_store = dsm_paths['ARCHIVE_STORAGE']
resource_path = dsm_paths['STAGING_RESOURCE']
warehouse_path = dsm_paths['STAGING_DATA']
staging_tools = dsm_paths['STAGING_TOOLS']
wh_root = dsm_paths['STAGING_DATA']


# NOTE: caseid is typically the first 3 fields of the native dataset_id, which
# usually corresponds to the first 3 fields of the datafile name.  It is not
# known which should take precedence if they differ.

def get_caseid(nat_src):
    anyfile = get_first_nc_file(nat_src)
    fname = os.path.basename(anyfile)
    selected = fname.split('.')[0:3]
    caseid = '.'.join(selected)
    return caseid

# comps = [Project . ModelVersion . Experiment . Resolution . Realm . Grid . DataType . Frequency . Ensemble]
# obtains namefile and restart dataset_ds corresponding to the native model-output dataset_id supplied.
# if dataset realm is sea-ice, must substitute the ocean restart dataset_id

def derive_namefile_and_restart_dsids(native_dsid):
    ret_dsids = []
    comps = native_dsid.split('.')
    if comps[4] in ["ocean", "sea-ice"] and comps[6] == "model-output":
        comps[6] = "namefile"
        comps[7] = "fixed"
        ret_dsids.append('.'.join(comps))
        comps[6] = "restart"
        if comps[4] == "sea-ice":
            comps[4] = "ocean"
        ret_dsids.append('.'.join(comps))

    return ret_dsids
        



# -----------------------------------------------------------
# Management Support Functions
# -----------------------------------------------------------

mainlog = ""

def dump_transfer_spec(nat_dsid: str, spec: list):
    log_message("info", f"    Transfer_spec for dsid: {nat_dsid}")
    log_message("info", f"    Dst Path = {spec['dst_path']}")
    log_message("info", f"    Src Path = {spec['src_path']}")
    log_message("info", f"    ArchSize = {spec['arch_size']}")


def support_in_remote_archive_map(nat_dsid: str, local_map_lines: list, remote_transfer_specs: list):

    NERSC_arch_map = os.path.join(archive_manage, "NERSC_Archive_Map")

    for aline in local_map_lines:
        local_arch_path = aline.split(',')[2]
        arch_name = os.path.basename(local_arch_path)
        log_message("info", f"DEBUG: seeking archive name {arch_name} in remote_archive_map")
        remote_map_line = lines_match(NERSC_arch_map, arch_name, ',', 1)[0]
        if remote_map_line != None:
            arch_size = int(remote_map_line.split(',')[2])
            remote_arch_path = remote_map_line.split(',')[5]
            spec = dict()
            spec['nat_dsid'] = nat_dsid
            spec['dst_path'] = local_arch_path
            spec['arch_size'] = arch_size
            spec['src_path'] = remote_arch_path

            dump_transfer_spec(nat_dsid, spec)

            remote_transfer_specs.append(spec)

    if len(remote_transfer_specs) == 0:
        return False
    return True

def retrieve_remote_archives(remote_transfer_specs):

    # if spacemode == "SMALL", tect 2x archive size to disk free-space
    # if space-ok, issue zstash (or globus_sdk) commands to retrive archive

    transfer_specs = list()
    remote_arch_size = 0
    for spec in remote_transfer_specs:
        remote_arch_size += (int(spec['arch_size']) * (2**10))
        remote_arch_path = spec['src_path']
        arch_name = os.path.basename(remote_arch_path)
        transfer_specs.append(tuple([remote_arch_path,spec['dst_path']]))

    total, used, free = shutil.disk_usage(archive_store)

    # log_message("info", f"SIZETEST: Remote Archive {arch_name} size = {remote_arch_size} GB.")
    if 2*remote_arch_size > (free / (2**30)):
        log_message("info", f"SIZETEST: Archive extraction would exceed disk free space {free // (2**30)} GB. archive={arch_name} ({remote_arch_size} GB)")
        return False
    else:
        log_message("info", f"SIZETEST: Archive extraction would NOT exceed disk free space {free // (2**30)} GB. archive={arch_name} ({remote_arch_size} GB)")
        
    # Conduct zstash globus transfer (or globus_sdk-based transfer) here.

    for spec in transfer_specs:
        log_message("info", f"SEEKING Remote Archive {arch_name} size = {remote_arch_size} GB.")
        log_message("info", f"Archive extraction faces disk free space {free // (2**30)} GB.")
        # transfer_status = globus_get("NERSC", spec[0], "LCRC", spec[1])
        
        # WORKWORKWORK
        # NERSC_HPSS
        UUID = "9cd89cfd-6d04-11e5-ba46-22000b92c6ec"
        cache = "/lcrc/group/e3sm/DSM/tmp/cache_test"
        zsrc = f"--hpss=globus://{UUID}/{spec[0]}"
        zdst = f"{spec[1]}"
        os.makedirs(zdst, exist_ok=True)

        cmd = ['zstash', 'check', zsrc, '--tars', '000000-', '--keep', '--cache', zdst]
        cmdstr = f"zstash check {zsrc} --tars 000000- --keep --cache {zdst}"
        log_message("info", f"cmd = {cmd}")
        log_message("info", f"cmdstr = {cmdstr}")
        cmd_result = subprocess.run(cmd, capture_output=True, text=True)
        if cmd_result.returncode != 0:
            log_message("info", f"ERROR: zstash FAIL to check/fetch archive {spec[0]}")
            log_message("info", f"STDERR: {cmd_result.stderr}")
            return False

    log_message("info", "Successfully retrieved remote archive")

    return True

def support_in_local_archive_map(native_dsid: str, map_lines: list):

    # test that native_dsid appears in local (E3SM) Archive_Map

    e3sm_arch_map = os.path.join(archive_manage, "Archive_Map")
    map_lines.extend(lines_match(e3sm_arch_map, native_dsid, ',', 1))
    if len(map_lines) == 0:
        log_message("info", f"No entries for {native_dsid} in {e3sm_arch_map}")
        return False
    return True

def support_in_local_archive(map_lines):

    # test that archive map_line paths point to existing local archives.

    if len(map_lines) == 0:
        return False
    support = True
    for aline in map_lines:
        arch_path = aline.split(',')[2]
        log_message("info", f"Testing for archive path: {arch_path}")
        if not os.path.exists(arch_path):
            log_message("info", f"Local archive path Not Found: {arch_path}")
            support = False
        if os.path.isdir(arch_path) and not os.listdir(arch_path):
            log_message("info", f"Local archive path has No Files: {arch_path}")
            support = False
    return support

def extract_from_local_archive(native_dsid):

    # use local (E3SM) Archive_Map to obtain archive path and search-pattern
    # use zstash to extract native dataset to the warehouse

    e3sm_arch_map = os.path.join(archive_manage, "Archive_Map")
    map_lines = lines_match(e3sm_arch_map, native_dsid, ',', 1)

    # only has effect if realm = ocean or sea-ice
    aux_dsids = derive_namefile_and_restart_dsids(native_dsid)
    for a_dsid in aux_dsids:
        aux_map_lines = lines_match(e3sm_arch_map, a_dsid, ',', 1)
        map_lines.extend(aux_map_lines)

    # DEBUGGING
    for aline in map_lines:
        log_message("info", f"DEBUG: Archive_Map line POST-extend: {aline}")



    realm = native_dsid.split('.')[0]

    # log_message("debug", f"map_lines = {map_lines}")
    # print(f"DEBUG: map_lines = {map_lines}", flush=True)

    if len(map_lines) == 0:
        log_message("info", f"No entries for {native_dsid} in {e3sm_arch_map}")
        return False
    
    arch_size = 0
    for aline in map_lines:
        log_message("info", f"DEBUG: parsing Archive_Map line: {aline}")
        arch_path = aline.split(',')[2]
        if os.path.exists(arch_path):
            asize = get_directory_content_size(arch_path)
            if asize == 0:
                log_message("info", f"Archive path {arch_path} has no files")
                return False
            arch_size += asize
        else:
            log_message("info", f"Archive path {arch_path} does not exist")
            return False
    
    # NOTE: We could be smarer here, and parse "zstash ls -l" to see if the 
    # extracted dataset alone would put us over-the-line, space-wise, as
    # opposed to using the entire archive size.

    total, used, free = shutil.disk_usage(archive_store)

    if arch_size > free:
        log_message("info", f"Archive {map_lines[0]} size = {arch_size // (2**30)} GB.")
        log_message("info", f"Archive extraction would exceed disk free space {free // (2**30)} GB.")
        return False
        
    # Conduct zstash extraction here

    for aline in map_lines:
        # for MPAS, cannot assume just model-output dsid
        arch_dsid = aline.split(',')[1]
        dsid_path = arch_dsid.replace(".", "/")
        warehouse_dest = os.path.join(warehouse_path, dsid_path, "v0")

        arch_path = aline.split(',')[2]
        arch_patt = aline.split(',')[3]
        log_message("info", f"Attempting zstash extract from archive {arch_path} the files {arch_patt}")
        full_patt = os.path.join(os.getcwd(), arch_patt)
        precount = len(glob.glob(full_patt))
        if precount > 0:
            log_message("info", f"NOTE: Extraction output target {arch_patt} already contains {precount} files.")
        cmd = ['zstash', 'extract', '--hpss=none', "--cache", f'{arch_path}', f"{arch_patt}"]
        log_message("info", f"Attempting extraction: {cmd}")
        cmd_result = subprocess.run(cmd, capture_output=True, text=True)
        if cmd_result.returncode != 0:
            log_message("info", f"ERROR: zstash FAIL to extract local dataset {arch_dsid}")
            log_message("info", f"STDERR: {cmd_result.stderr}")
            return False
        if not glob_move(full_patt, warehouse_dest):
            return False
        
    return True

def support_in_warehouse(native_dsid):

    native_src = latest_data_vdir(native_dsid)
    if os.path.exists(native_src):
        if get_directory_content_size(native_src) > 0:
            return True
    return False
    


# Setup Global Vars and Paths

the_pwd = os.getcwd()
gv_workdir = os.path.realpath(the_pwd)
gv_tmp_dir = os.path.join(f"{gv_workdir}", "tmp")
gv_tmp_statdir = os.path.join(f"{gv_tmp_dir}", "stat")
os.makedirs(gv_tmp_dir, exist_ok=True)
os.makedirs(gv_tmp_statdir, exist_ok=True)

spacemode = "LARGE"
dryrun = True

# Maintain 3 queues and output files: stat_pending, stat_success, stat_failure.

class ManageRunstatus():

    def __init__(self, dsidlist):
        ts = get_UTC_TS()
        self.stat_pending = deque(dsidlist)
        self.stat_success = deque()
        self.stat_failure = deque()
        self.stat_pending_file = os.path.join(gv_tmp_statdir, f"stat-{ts}-pending")
        self.stat_success_file = os.path.join(gv_tmp_statdir, f"stat-{ts}-success")
        self.stat_failure_file = os.path.join(gv_tmp_statdir, f"stat-{ts}-failure")
        write_file_list(self.stat_pending_file, list(self.stat_pending))
        write_file_list(self.stat_success_file, list(self.stat_success))
        write_file_list(self.stat_failure_file, list(self.stat_failure))

    def update(self, status="UNKNOWN", dsid=None):
        dsid = self.stat_pending.popleft()
        if status == "SUCCESS":
            self.stat_success.append(dsid)
            write_file_list(self.stat_success_file, list(self.stat_success))
        if status == "FAILURE":
            self.stat_failure.append(dsid)
            write_file_list(self.stat_failure_file, list(self.stat_failure))
        write_file_list(self.stat_pending_file, list(self.stat_pending))
        

def manage_cmip6_workflow(dsids: list, pargs: argparse.Namespace):

    runstatus = ManageRunstatus(dsidlist=dsids)
    
    task_count = len(dsids)
    tasks_done = 0
    for dsid in dsids:
        log_message("info", f"============ Attempting CMIP6 generation for dataset {dsid}")
        nat_dsid = parent_native_dsid(dsid)
        if not support_in_warehouse(nat_dsid):
            map_lines = list()
            if not support_in_local_archive_map(nat_dsid, map_lines):
                log_message("info", f"DEBUG: NO support in local archive map for {nat_dsid}")
                runstatus.update("FAILURE", dsid)
                continue

            if len(map_lines) == 0:
                log_message("info", "ERROR: support_in_local_archive_map returned True, but no map_lines")
                continue

            log_message("info", f"FOUND support in LOCAL archive map: {map_lines[0]}")

            if not support_in_local_archive(map_lines):
                log_message("info", f"DEBUG: NO support in local archive, checking for remote {nat_dsid}")
                remote_map_lines = list()
                if not support_in_remote_archive_map(nat_dsid, map_lines, remote_map_lines):
                    log_message("info", f"DEBUG: NO support in remote archive map for {nat_dsid}")
                    runstatus.update("FAILURE", dsid)
                    continue
                if not retrieve_remote_archives(remote_map_lines):
                    log_message("info", f"Cannot retrieve remote archive for native data {nat_dsid}")
                    log_message("info", f"Cannot process CMIP6 dataset {dsid}")
                    runstatus.update("FAILURE", dsid)
                    continue
            log_message("info", f"Proceeding to extract from local archives")
            if not extract_from_local_archive(nat_dsid):
                log_message("info", f"Cannot extract native dataset {nat_dsid} from local archive to warehouse")
                log_message("info", f"Cannot process CMIP6 dataset {dsid}")
                runstatus.update("FAILURE", dsid)
                continue
            log_message("info", f"Proceeding to generate CMIP6")
        
        log_message("info", f"DEBUG: Found support in warehouse for {nat_dsid}")
        dsidfile = os.path.join(gv_workdir, f"dsm_gen-{dsid}")
        quiet_remove(dsidfile)
        fappend(dsidfile, f"{dsid}")
        dsmgenCMIP6 = os.path.join(staging_tools, "dsm_generate_CMIP6.py")
        cmd = ["python", f"{dsmgenCMIP6}", "--runmode", f"{pargs.run_mode}", "-i", f"{dsidfile}"]
        log_message("info", f"CMD = {cmd}")

        cmd_result = subprocess.run(cmd, capture_output=True, text=True)
        if cmd_result.returncode != 0:
            log_message("info", f"ERROR: {dsmgenCMIP6} FAIL to generate CMIP dataset {dsid}")
            log_message("info", f"STDERR: {cmd_result.stderr}")
            runstatus.update("FAILURE", dsid)
            continue
        else:
            tasks_done += 1
            log_message("info", f"Successful generation of CMIP dataset {dsid} ({tasks_done} of {task_count})")
            runstatus.update("SUCCESS", dsid)

    log_message("info", f"Processed {task_count} dataset_ids")


def main():
    global mainlog

    pargs = assess_args()

    print(f"DEBUG: pargs = {pargs}")

    targ_list = os.path.basename(pargs.input_dsids)
    gv_log_dir = os.path.join(gv_tmp_dir, "mainlogs")
    os.makedirs(gv_log_dir, exist_ok=True)

    ts = get_UTC_TS()
    mainlog = os.path.join(gv_log_dir, f"{targ_list}.log-{ts}")
    setup_logging("info", mainlog)
    print(f"DEBUG: mainlog = {mainlog}", flush=True)

    dsid_list = load_file_lines(pargs.input_dsids)

    manage_cmip6_workflow(dsid_list, pargs)
    

# Entry point of the program
if __name__ == "__main__":
    main()

