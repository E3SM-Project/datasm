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
from datasm.util import setup_logging
from datasm.util import log_message
from datasm.util import dirlist
from datasm.util import dircount
from datasm.util import get_dsm_paths
from datasm.util import load_file_lines
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
               export DSM_GETPATH=/p/user_pub/e3sm/staging/Relocation/.dsm_get_root_path.sh
           into your .bashrc file, so that configs, mapfiles, metadata can be located.

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

def first_regular_file(directory):
    """Return the first regular file in the specified directory or None."""
    try:
        for entry in os.listdir(directory):
            path = os.path.join(directory, entry)
            if os.path.isfile(path):  # Check if it's a regular file
                return path  # Return the first regular file found
    except FileNotFoundError:
        print(f"The directory '{directory}' does not exist.")
    except PermissionError:
        print(f"Permission denied to access '{directory}'.")
    
    return None  # Return None if no regular file is found

def find_first_line_with_term(file_name, search_term):
    """Return the first line in the file that contains the given search term."""
    with open(file_name, 'r') as file:
        for line in file:
            if search_term in line:
                return line.strip()  # Return the line without leading/trailing whitespace
    return None  # Return None if no line contains the search term

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

def linex_list(path: str, keyv: str, delim: chr, pos: int):
    retlist = list()
    if not os.path.exists(path):
        print(f"ERROR: linex: no path {path}")
        return retlist
    with open(path, 'r') as thefile:
        for aline in thefile:
            if keyv in aline:
                if delim == "":
                    retlist.append(aline.rstrip())
                else:
                    retlist.append(aline.split(delim)[pos].rstrip())
    return retlist

def quiet_remove(afile):
    if os.path.isfile(afile):
        os.remove(afile)

def clear_files(directory):
    """Delete all files in the specified directory."""
    try:
        for entry in os.listdir(directory):
            path = os.path.join(directory, entry)
            if os.path.isfile(path):  # Check if it's a regular file
                os.remove(path)  # Delete the file
    except Exception as e:
        print(f"clear_files: An error occurred: {e}")

def delete_dir(directory):
    """Remove a directory and all its contents recursively."""
    try:
        if os.path.exists(directory):
            shutil.rmtree(directory)  # Remove the directory and all its contents
    except Exception as e:
        print(f"delete_dir: An error occurred: {e}")

def copy_file(source, destination):
    """Copy a file from source to destination."""
    try:
        shutil.copy(source, destination)  # Copy the file
    except FileNotFoundError:
        print(f"The file '{source}' does not exist.")
    except PermissionError:
        print(f"Permission denied to copy '{source}' to '{destination}'.")
    except Exception as e:
        print(f"An error occurred: {e}")

def glob_move(src_pattern, dest_dir):
    os.makedirs(dest_dir, exist_ok=True)

    # Use glob to find all files matching the pattern
    files_to_move = glob.glob(src_pattern)

    # Move each file to the destination directory
    for file_path in files_to_move:
        try:
            shutil.move(file_path, dest_dir)
        except Exception as e:
            print(f"An error occurred while moving '{file_path}': {e}")

def force_symlink(target, link_name):
    if os.path.islink(link_name):
        os.unlink(link_name)
    os.symlink(target, link_name)

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


# -----------------------------------------------------------
# Management Support Functions
# -----------------------------------------------------------

mainlog = ""

def retrieve_remote_archives(native_dsid):

    # employ NERSC_Archive_Map to obtain remote path information
    # if spacemode == "SMALL", tect 2x archive size to disk free-space
    # if space-ok, issue zstash (or globus_sdk) commands to retrive archive

    e3sm_arch_map = os.path.join(archive_manage, "Archive_Map")
    NERSC_arch_map = os.path.join(archive_manage, "NERSC_Archive_Map")

    e3sm_map_lines = linex_list(e3sm_arch_map, native_dsid, ',', 1)
    if len(e3sm_map_lines) == 0:
        log_message("info", f"No entries for {native_dsid} in {e3sm_arch_map}")
        return False

    NERSC_map_lines = list()
    remote_arch_paths = list()
    remote_arch_size = 0
    for aline in e3sm_map_lines:
        arch_path = aline.split(',')[2]
        arch_name = os.path.basename(arch_path)
        NERSC_map_line = linex_list(e3sm_arch_map, arch_name, ',', 1)[0]
        remote_arch_size += int(NERSC_map_line.split(',')[2])
        remote_arch_path = NERSC_map_line.split(',')[5]
        remote_arch_paths.append(remote_arch_path )

    total, used, free = shutil.disk_usage(archive_store)

    if 2*remote_arch_size > free:
        log_message("info", f"Remote Archive {arch_name} size = {remote_arch_size // (2**30)} GB.")
        log_message("info", f"Archive extraction would exceed disk free space {free // (2**30)} GB.")
        return False

    # Condust zstash globust transfer (or globus_sdk-based transfer) here.



    return True

def support_in_local_archive(native_dsid):

    # test that native_dsid appears in local (E3SM) Archive_Map

    e3sm_arch_map = os.path.join(archive_manage, "Archive_Map")
    map_lines = linex_list(e3sm_arch_map, native_dsid, ',', 1)
    if len(map_lines) == 0:
        log_message("info", f"No entries for {native_dsid} in {e3sm_arch_map}")
        return False
    return True

def extract_from_local_archive(native_dsid):

    # use local (E3SM) Archive_Map to obtain archive path and search-pattern
    # use zstash to extract native dataset to the warehouse

    e3sm_arch_map = os.path.join(archive_manage, "Archive_Map")
    map_lines = linex_list(e3sm_arch_map, native_dsid, ',', 1)
    if len(map_lines) == 0:
        log_message("info", f"No entries for {native_dsid} in {e3sm_arch_map}")
        return False
    
    arch_size = 0
    for aline in map_lines:
        arch_path = aline.split(',')[2]
        if os.path.exists(arch_path):
            asize += get_directory_content_size(arch_path)
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

    dsid_path = native_dsid.replace(".", "/")
    warehouse_dest = os.path.join(warehouse_path, dsid_path, "v0")

    for aline in map_lines:
        arch_path = aline.split(',')[2]
        arch_patt = aline.split(',')[3]
        full_patt = os.path.join(arch_path, arch_patt)
        cmd = ['zstash', 'extract', '--hpss=none', f'{full_patt}']
        cmd_result = subprocess.run(cmd, capture_output=True, text=True)
        if cmd_result.returncode != 0:
            log_message("info", f"ERROR: zstash FAIL to extract local dataset {inative_dsid}")
            log_message("info", f"STDERR: {cmd_result.stderr}")
            return False
        glob_move(full_patt, warehouse_dest)
        
    return True

def support_in_warehouse(native_dsid):

    native_src = latest_data_vdir(native_dsid)
    if os.path.exists(native_src):
        return True
    return False
    


# Setup Global Vars and Paths

the_pwd = os.getcwd()
gv_workdir = os.path.realpath(the_pwd)
gv_tmp_dir = f"{gv_workdir}/tmp"
os.makedirs(gv_tmp_dir, exist_ok=True)

spacemode = "LARGE"
dryrun = True

def manage_cmip6_workflow(dsids: list, pargs: argparse.Namespace):

    for dsid in dsids:
        log_message("info", f"Attempting CMIP6 generation for dataset {dsid}")
        nat_dsid = parent_native_dsid(dsid)
        if not support_in_warehouse(nat_dsid):
            if not support_in_local_archive(nat_dsid):
                if not retrieve_remote_archives(nat_dsid):
                    log_message("info", f"Cannot retrieve remote archive for native data {nat_dsid}")
                    log_message("info", f"Cannot process CMIP6 dataset (dsid)")
                    continue
            if not extract_from_local_archive(nat_dsid):
                log_message("info", f"Cannot extract native dataset {nat_dsid} from local archive")
                log_message("info", f"Cannot process CMIP6 dataset (dsid)")
                continue
        
        cmd = ["python", f"{dsmgenCMIP6}", "--runmode", f"{pargs.runmode}", "-i", f"dsidfile", "--info-out", e2c_info_yaml]
        cmd_result = subprocess.run(cmd, capture_output=True, text=True)
        if cmd_result.returncode != 0:
            log_message("info", f"ERROR: {dsmgenCMIP6} FAIL to generate CMIP dataset {dsid}")
            log_message("info", f"STDERR: {cmd_result.stderr}")
            continue
                



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

