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
from datasm.util import write_file_list
from datasm.util import fappend
from datasm.util import latest_data_vdir
from datasm.util import get_first_nc_file
from datasm.util import ensure_status_file_for_dsid
from datasm.util import get_UTC_TS


helptext = '''
    Usage: archive_extraction_by_dsid.py <native_dataset_id>> 

        The local (E3SM) Archive_Map is consulted, and any extaction paths and patterns found
        for the given dataset_id are passed to zstash for extraction to the "warehouse"

        The extraction will not proceed if the archive size exceeds the apace available in
        the destination partition.

    Note: The runtime environment must include the following items:

        1. (suggestion) Use conda/mamba create env -n <name> -f datasm/conda-env/prod.yaml
           to create the runtime environment.
        2. pip install datasm (ensures that datasm.utils are available along with configs)

        3. Place the line
               export DSM_GETPATH=<path_to_DSM_STAGING>/Relocation/.dsm_get_root_path.sh
           into your .bashrc file, so that configs, mapfiles, metadata can be located.
           (on Chrysalis, DSM_STAGING = /lcrc/group/e3sm2/DSM/Staging)

        4. pip install zstash (or ensure it exists in your environment).

'''

def assess_args():

    parser = argparse.ArgumentParser(description=helptext, prefix_chars='-', formatter_class=RawTextHelpFormatter)
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')
    required.add_argument('-i', '--input_dsid', action='store', dest="input_dsid", type=str, help="native dataset_id", required=True)

    args = parser.parse_args()

    return args

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



def extract_from_local_archive(native_dsid):

    # use local (E3SM) Archive_Map to obtain archive path and search-pattern
    # use zstash to extract native dataset to the warehouse

    e3sm_arch_map = os.path.join(archive_manage, "Archive_Map")
    map_lines = lines_match(e3sm_arch_map, native_dsid, ',', 1)

    # log_message("debug", f"map_lines = {map_lines}")
    # print(f"DEBUG: map_lines = {map_lines}", flush=True)

    if len(map_lines) == 0:
        log_message("info", f"No entries for {native_dsid} in {e3sm_arch_map}")
        return False

    arch_size = 0
    for aline in map_lines:
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

    dsid_path = native_dsid.replace(".", "/")
    warehouse_dest = os.path.join(warehouse_path, dsid_path, "v0")

    for aline in map_lines:
        arch_path = aline.split(',')[2]
        arch_patt = aline.split(',')[3]
        log_message("info", f"Attempting zstash extract from archive {arch_path} the files {arch_patt}")
        full_patt = os.path.join(os.getcwd(), arch_patt)
        precount = len(glob.glob(full_patt))
        if precount > 0:
            log_message("info", f"NOTE: Extraction output target {arch_patt} already contains {precount} files.")
        cmd = ['zstash', 'extract', '--hpss=none', "--cache", f'{arch_path}', f"{arch_patt}"]
        cmd_result = subprocess.run(cmd, capture_output=True, text=True)
        if cmd_result.returncode != 0:
            log_message("info", f"ERROR: zstash FAIL to extract local dataset {native_dsid}")
            log_message("info", f"STDERR: {cmd_result.stderr}")
            return False
        if not glob_move(full_patt, warehouse_dest):
            return False

    return True


def main():
    global mainlog

    pargs = assess_args()

    print(f"DEBUG: pargs = {pargs}")

    target_dsid = os.path.basename(pargs.input_dsid)

    ts = get_UTC_TS()
    mainlog = f"extraction_log-{target_dsid}.log-{ts}"
    setup_logging("info", mainlog)
    print(f"DEBUG: mainlog = {mainlog}", flush=True)

    retval = extract_from_local_archive(target_dsid)

    if retval == True:
        log_message("info", f"Extraction SUCCESS: {target_dsid}")
    else:
        log_message("info", f"Extraction FAILURE: {target_dsid}")


# Entry point of the program
if __name__ == "__main__":
    main()

