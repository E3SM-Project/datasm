#!/usr/bin/env python3

# Import necessary modules
import sys
import os
import glob
import shutil
import argparse
import json
import yaml
import fnmatch
import subprocess
from argparse import RawTextHelpFormatter
from time import sleep
from datasm.util import setup_logging
from datasm.util import log_message
from datasm.util import dirlist
from datasm.util import dircount
from datasm.util import get_dsm_paths
from datasm.util import load_file_lines
from datasm.util import fappend
from datasm.util import force_symlink
from datasm.util import dsid_to_dict
from datasm.util import parent_native_dsid
from datasm.util import latest_data_vdir
from datasm.util import get_first_nc_file
from datasm.util import ensure_status_file_for_dsid
from datasm.util import get_UTC_TS
from datasm.util import tss

helptext = '''
    Usage: dsmgen_cmip <runmode> <file_of_cmip6_dataset_ids> [--dryrun] [alternate_dataset_spec.yaml]
        <runmode> must be either "TEST" or "WORK".
        In TEST mode, only the first year of data will be processed,
        and the E3SM dataset status files are not updated."
        In WORK mode, all years given in the dataset_spec are applied,
        and the E3SM dataset status files are updated, and the cmorized
        results are moved to staging data (the warehouse).

        if "--dryrun" is given, the subordinate python run-script is created but not executed.

    Note: The runtime environment must include the following items:

        1. (suggestion) Use conda/mamba create env -n <name> -f datasm/conda-env/prod.yaml
           to create the runtime environment.
        2. pip install datasm (ensures that datasm.utils are available along with configs)

        3. Place the line
               export DSM_GETPATH=<path_to_DSM_STAGING>/Relocation/.dsm_get_root_path.sh
           into your .bashrc file, so that configs, mapfiles, metadata can be located.
           (on Chrysalis, DSM_STAGING = /lcrc/group/e3sm2/DSM/Staging)

        4. pip install e3sm_to_cmip

        5. The NCO tools "ncclimo" and "ncremap" must exist in the environment.

'''

def assess_args():

    parser = argparse.ArgumentParser(description=helptext, prefix_chars='-', formatter_class=RawTextHelpFormatter)
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')
    required.add_argument('-i', '--input_dsid_list', action='store', dest="input_dsids", type=str, help="file list of CMIP dataset_ids", required=True)
    required.add_argument('--runmode', action='store', dest="run_mode", type=str, help="\"TEST\" or \"WORK\"", required=True)
    required.add_argument('--dryrun', action='store_true', dest="dryrun", help="(do not run created subscript)", required=False)
    optional.add_argument('--ds_spec', action='store', dest="alt_ds_spec", type=str, help="alternative dataset spec", required=False)

    args = parser.parse_args()

    return args


# -----------------------------------------------------------
# A Simple Utility
# -----------------------------------------------------------

def tss():
    # timestamp in epoch seconds
    return int(datetime.now().timestamp())


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

# -----------------------------------------------------------
# Convenient Function Definitions
# -----------------------------------------------------------

def table_realm(table: str):

    if table in ["3hr", "AERmon", "Amon", "CFmon", "day", "fx"]:
        return "atm"
    elif table in ["Lmon", "LImon"]:
        return "lnd"
    elif table in ["Ofx", "Omon"]:
        return "mpaso"
    elif table == "SImon":
        return "mpassi"
    else:
        return "UNKN_REALM_FOR_" + table

def verbose_realm(realm_code: str):
    if realm_code == "atm":
        return "atmos"
    if realm_code == "lnd":
        return "land"
    if realm_code == "mpaso":
        return "ocean"
    if realm_code == "mpassi":
        return "sea-ice"
    return "UNKNOWN"

def table_freq(table: str):

    if table in ["3hr", "day", "6hrLev", "6hrPlev", "6hrPlevPt", "1hr"]:
        return table
    else:   # Amon, CFmon, Lmon, Omon, SImon, etc
        return "mon"

# Distinguished variable types

CVatm3d = ["hus", "o3", "ta", "ua", "va", "zg", "hur", "wap"]
CVatmfx = ["areacella", "orog", "sftlf"]
CVatmdy = ["tasmin", "tasmax", "tas", "huss", "rlut", "pr"]
CVatm3h = ["pr"]
CVlnd = ["mrsos", "mrso", "mrfso", "mrros", "mrro", "prveg", "evspsblveg", "evspsblsoi", "tran", "tsl", "lai"]
CVmpaso = ["areacello", "fsitherm", "hfds", "masso", "mlotst", "sfdsi", "sob", "soga", "sos", "sosga", "tauuo", "tauvo", "thetaoga", "tob", "tos", "tosga", "volo", "wfo", "zos", "thetaoga", "hfsifrazil", "masscello", "so", "thetao", "thkcello", "uo", "vo", "volcello", "wo", "zhalfo"]
CVmpassi = ["siconc", "sitemptop", "sisnmass", "sitimefrac", "siu", "siv", "sithick", "sisnthick", "simass"]

def var_type_code(var: str, realm: str, freq: str):

    if realm == "atm":
        if freq == "mon":
            if var in CVatm3d:
                return "atm_mon_3d"
            if var in CVatmfx:
                return "atm_mon_fx"
            else:
                return "atm_mon_2d"     # default for mon
        elif freq == "day" and var in CVatmdy:
            return "atm_day"
        elif freq == "3hr" and var in CVatm3h:
            return "atm_3hr"
        else:
            return "NONE"
    elif realm == "lnd":
        if freq == "mon":
            if var in CVlnd:
                return "lnd_mon"
            elif var == "snw":
                return "lnd_ice_mon"
            else:
                return "NONE"
        else:
            return "NONE"
    elif realm == "mpaso":
        if freq == "mon":
            if var in CVmpaso:
                return "mpaso_mon"
            else:
                return "NONE"
        else:
            return "NONE"
    elif realm == "mpassi":
        if freq == "mon":
            if var in CVmpassi:
                return "mpassi_mon"
            else:
                return "NONE"
        else:
            return "NONE"
    else:
        return "NONE"
        

def suggested_ypf(vartypecode: str):
    if vartypecode in ["lnd_mon", "atm_mon_2d", "atm_mon_3d", "atm_mon_fx"]:
        return 10
        # return 50
    return 10

dsm_paths = get_dsm_paths()
resource_path = dsm_paths['STAGING_RESOURCE']
staging_tools = dsm_paths['STAGING_TOOLS']
staging_data = dsm_paths['STAGING_DATA']
maps_path = os.path.join(resource_path, "maps")
cmor_tables = os.path.join(resource_path, "cmor/cmip6-cmor-tables/Tables")
derive_conf = os.path.join(resource_path, "derivatives.conf")
metadata_version = os.path.join(staging_tools, "metadata_version.py")
run_reporter = os.path.join(staging_tools, "produce_dsid_run_record.sh")

vrt_remap_plev19=os.path.join(f"{resource_path}", "grids", "vrt_remap_plev19.nc")


# NOTE: caseid is typically the first 3 fields of the native dataset_id, which
# usually corresponds to the first 3 fields of the datafile name.  It is not
# known which should take precedence if they differ.

def get_caseid(nat_src):
    anyfile = get_first_nc_file(nat_src)
    fname = os.path.basename(anyfile)
    selected = fname.split('.')[0:3]
    caseid = '.'.join(selected)
    return caseid

def get_metadata_file_version(metadata):
    with open(metadata, "r") as file_content:
        json_in = json.load(file_content)
        return json_in["version"]

def get_sim_years(dsid: str, altspec: str):
    dsm_paths = get_dsm_paths()
    if altspec:
        spec_path = altspec
    else:
        resource_path = dsm_paths['STAGING_RESOURCE']
        spec_path = os.path.join(resource_path, 'dataset_spec.yaml')

    with open(spec_path, 'r') as instream:
        dataset_spec = yaml.load(instream, Loader=yaml.SafeLoader)
    dc = dsid.split(".")
    the_experiment_record = dataset_spec['project'][dc[0]][dc[1]][dc[2]]

    return the_experiment_record['start'], the_experiment_record['end']

def get_namefile(dsid: str):
    headpart = dsid.split('.')[0:6]
    tailpart = dsid.split('.')[8:9]
    namedsid = '.'.join(headpart + ["namefile", "fixed"] + tailpart)
    # log_message("info", f"DEBUG: get_namefile(): namedsid = {namedsid}")
    namevdir = latest_data_vdir(namedsid)
    if os.path.exists(namevdir):
        return first_regular_file(namevdir)
    return "NONE"

def get_restfile(dsid: str):
    headpart = dsid.split('.')[0:6]
    tailpart = dsid.split('.')[8:9]
    restdsid = '.'.join(headpart + ["restart", "fixed"] + tailpart)
    # if sea-ice, use the ocean restart
    restdsid = restdsid.replace("sea-ice", "ocean")
    # log_message("info", f"DEBUG: get_restfile(): restdsid = {restdsid}")
    restvdir = latest_data_vdir(restdsid)
    if os.path.exists(restvdir):
        return first_regular_file(restvdir)
    return "NONE"

def get_regrid_map(dsid: str):
    dsd = dsid_to_dict("NATIVE", dsid)
    selection_key=f"{dsd['realm']},{dsd['model']},{dsd['resolution']},REGRID"
    retval = linex(derive_conf, selection_key, ',', 4)
    if retval == "":
        return "NONE"
    return os.path.join(maps_path, retval)
    

def get_region_mask(dsid: str):
    dsd = dsid_to_dict("NATIVE", dsid)
    selection_key=f"{dsd['realm']},{dsd['model']},{dsd['resolution']},MASK"
    retval = linex(derive_conf, selection_key, ',', 4)
    if retval == "":
        return "NONE"
    return os.path.join(maps_path, retval)

def get_mpas_file_year(afile):
    return afile.split('.')[-2].split('-')[0]

# Establish target symlinks

def setup_target_symlinks(runmode, the_var_type, native_src, native_data, realm, restartf, namefile, region_f):

    # produce symlinks to native source in native_data
    natsrc_list = dirlist(native_src)
    if the_var_type == "atm_mon_fx":
        # special case, just 1 file whether TEST or WORK mode
        afile = os.path.basename(natsrc_list[0])
        force_symlink( natsrc_list[0], os.path.join(native_data, afile) )
    elif runmode == "TEST":
        if the_var_type in ["mpaso_mon", "mpassi_mon"]:
            # obtain the "year code" for first year.  May be "0001", or "1850", etc 
            year_tag = get_mpas_file_year( os.path.basename(natsrc_list[0]) )
            pattern = f"*.{year_tag}-*.nc"
            matches = [s for s in natsrc_list if fnmatch.fnmatch(s, pattern)]               
            for amatch in matches:
                bmatch = os.path.basename(amatch)
                force_symlink( amatch, os.path.join(native_data, bmatch) )
        else:
            # NOTE: For non-MPAS, the regridding step accepts start and end year, which
            # automatically limits the input files presented to e3sm_to_cmip
            for afile in natsrc_list:
                bfname = os.path.basename(afile)
                force_symlink( afile, os.path.join(native_data, bfname) )
    else:   # WORK mode
        for afile in natsrc_list:
            bfname = os.path.basename(afile)
            force_symlink( afile, os.path.join(native_data, bfname) )

    # For MPAS, also place symlinks to restart, namefile and regionfile into the
    # native_data directory.

    if realm in ["mpaso", "mpassi"]:
        if restartf != "NONE":
            restart_base = os.path.basename(restartf)
            force_symlink(restartf, os.path.join(native_data, restart_base))
        if namefile != "NONE":
            namefile_base = os.path.basename(namefile)
            force_symlink(namefile, os.path.join(native_data, namefile_base))
        if region_f != "NONE":
            region_f_base = os.path.basename(region_f)
            force_symlink(region_f, os.path.join(native_data, region_f_base))

# Setup Global Vars and Paths

the_pwd = os.getcwd()
gv_workdir = os.path.realpath(the_pwd)
gv_tmp_dir = f"{gv_workdir}/tmp"
gv_yml_dir = f"{gv_tmp_dir}/info_yaml"
os.makedirs(gv_tmp_dir, exist_ok=True)
os.makedirs(gv_yml_dir, exist_ok=True)
mainlog = ""

gv_wtf_log = os.path.join(gv_workdir,"WTF")
gv_Reports = f"{gv_workdir}/CMIP6_RUN_REPORTS"
os.makedirs(gv_Reports, exist_ok=True)

def process_dsids(dsids: list, pargs: argparse.Namespace):

    for dsid in dsids:
        dsd = dsid_to_dict("CMIP6",dsid)
        freq = table_freq(dsd["table"])
        realm = table_realm(dsd["table"])
        nat_dsid = parent_native_dsid(dsid)
        native_src = latest_data_vdir(nat_dsid)
        the_var_type = var_type_code(dsd["cmip6var"], realm, freq) 
        caseid = get_caseid(native_src)

        print(f"CASE_ID = {caseid}")

        # initialize run_mode variations
        year_start, year_final = get_sim_years(nat_dsid, pargs.alt_ds_spec)

        status_file = ""
        ypf = 1
        if pargs.run_mode == "TEST":
            year_final = year_start
        elif pargs.run_mode == "WORK":
            ypf = suggested_ypf(the_var_type)
            status_file = ensure_status_file_for_dsid(dsid)
            # print(f"DEBUG: status_file = {status_file}", flush=True)

        print(f"StartYear = {year_start}, EndYear = {year_final}, YPF = {ypf}")
        e2c_info_yaml = f"{gv_yml_dir}/{dsd['table']}_{dsd['cmip6var']}.yaml"

        cmd = ["e3sm_to_cmip", "--info", "-v", dsd['cmip6var'], "--freq", freq, "--realm", realm, "-t", cmor_tables, "--map", "no_map", "--info-out", e2c_info_yaml]
        cmd_result = subprocess.run(cmd, capture_output=True, text=True)
        if cmd_result.returncode != 0:
            log_message("error", f"E2C INFO CMD FAILED: cmd = {cmd}")
            log_message("error", f"STDERR: {cmd_result.stderr}")
            continue

        nat_vars = linex(e2c_info_yaml,"E3SM Variables",':',1)
        nat_vars = ''.join(nat_vars.split(' ')) # remove whitespace
        namefile = get_namefile(nat_dsid)
        restartf = get_restfile(nat_dsid)
        map_file = get_regrid_map(nat_dsid)
        region_f = get_region_mask(nat_dsid)

        # begin persistent storage across cases.

        casedir = os.path.join(gv_workdir, "tmp", caseid)
        subscripts = os.path.join(casedir, "scripts")
        sublogs = os.path.join(casedir, "caselogs")
        native_data = os.path.join(casedir, "native_data")
        native_out = os.path.join(casedir, "native_out")
        rgr_dir = os.path.join(casedir, "rgr")
        rgr_dir_vert = os.path.join(casedir, "rgr_vert")
        result_dir = os.path.join(casedir, "product")

        delete_dir(native_data)
        delete_dir(native_out)
        delete_dir(rgr_dir)
        delete_dir(rgr_dir_vert)

        os.makedirs(casedir, exist_ok=True)
        os.makedirs(subscripts, exist_ok=True)
        os.makedirs(sublogs, exist_ok=True)
        os.makedirs(native_data, exist_ok=True)
        os.makedirs(native_out, exist_ok=True)
        os.makedirs(rgr_dir, exist_ok=True)
        os.makedirs(rgr_dir_vert, exist_ok=True)
        os.makedirs(result_dir, exist_ok=True)

        clear_files(native_data)
        clear_files(native_out)
        clear_files(rgr_dir)
        clear_files(rgr_dir_vert)

        if pargs.run_mode == "WORK":
            delete_dir(result_dir)
            os.makedirs(result_dir, exist_ok=True)

        # Copy correct metadata file and edit for version = TODAY

        metadata_dst = os.path.join(casedir, "metadata")
        os.makedirs(metadata_dst, exist_ok=True)
        metadata_name = f"{dsd['experiment']}_{dsd['variant_label']}.json"
        metadata_src = os.path.join(resource_path, "CMIP6-Metadata", f"{dsd['source_id']}", metadata_name)
        copy_file(metadata_src, metadata_dst)
        metadata_file = os.path.join(metadata_dst, metadata_name)
        cmd = ["python", metadata_version, "-i", metadata_file, "-m", "set"]
        cmd_result = subprocess.run(cmd, capture_output=True, text=True)
        if cmd_result.returncode != 0:
            log_message("error", f"Metadata Versioning CMD FAILED: cmd = {cmd}")
            log_message("error", f"STDERR: {cmd_result.stderr}")
            continue

        log_message("info", f"        ISSUED: {pargs.run_mode} {pargs.input_dsids}")
        log_message("info", f"      run_mode: {pargs.run_mode}")
        log_message("info", f"    CMIP6_dsid: {dsid}")
        log_message("info", f"   cmip_src_id: {dsd['source_id']}")
        log_message("info", f"   cmip_vlabel: {dsd['variant_label']}")
        log_message("info", f"    experiment: {dsd['experiment']}")
        log_message("info", f"        caseid: {caseid}")
        log_message("info", f"    year_start: {year_start}")
        log_message("info", f"    year_final: {year_final}")
        log_message("info", f"           ypf: {ypf}")
        log_message("info", f"         realm: {realm}")
        log_message("info", f"          freq: {freq}")
        log_message("info", f"      var_type: {the_var_type}")
        log_message("info", f"      cmip6var: {dsd['cmip6var']}")
        log_message("info", f"      nat_vars: {nat_vars}")
        log_message("info", f"    native_src: {native_src}")
        log_message("info", f"      namefile: {namefile}")
        log_message("info", f"      restartf: {restartf}")
        log_message("info", f"      map_file: {map_file}")
        log_message("info", f"      region_f: {region_f}")
        log_message("info", f"      metadata: {metadata_file}")
        log_message("info", f"   cmor_tables: {cmor_tables}")

        #
        # Create the var-type specific command lines ============================================
        #

        cmdflags = ["-7", "--dfl_lvl=1", "--no_cll_msr", "--no_stdin"]
        cmd_1 = cmd_1b = cmd_2 = []
        if the_var_type == "atm_mon_2d": 
            cmd_1 = ["ncclimo", "-P", "eam", "-j", "1", f"--map={map_file}", f"--start={year_start}", f"--end={year_final}", f"--ypf={ypf}", "--split", f"--caseid={caseid}", "-o", f"{native_out}", "-O", f"{rgr_dir}", "-v", f"{nat_vars}", "-i", f"{native_data}"]
            cmd_1.extend(cmdflags)
            cmd_2 = ["e3sm_to_cmip", "-v", f"{dsd['cmip6var']}", "--freq", f"{freq}", "-u", f"{metadata_file}", "-t", f"{cmor_tables}", "-o", f"{result_dir}", "-i", f"{rgr_dir}"]
        elif the_var_type == "atm_mon_fx": 
            cmd_1 = ["ncremap", f"--map={map_file}", "-v", f"{nat_vars}", "-I", f"{native_data}", "-O", f"{rgr_dir}", "--no_stdin"]
            cmd_2 = ["e3sm_to_cmip", "-v", f"{dsd['cmip6var']}", "-u", f"{metadata_file}", "-t", f"{cmor_tables}", "-o", f"{result_dir}", "-i", f"{rgr_dir}", "--realm", "fx"]
        elif the_var_type == "atm_mon_3d": 
            cmd_1 = ["ncclimo", "-P", "eam", "-j", "1", f"--map={map_file}", f"--start={year_start}", f"--end={year_final}", f"--ypf={ypf}", "--split", f"--caseid={caseid}", "-o", f"{native_out}", "-O", f"{rgr_dir_vert}", "-v", f"{nat_vars}", "-i", f"{native_data}"]
            cmd_1.extend(cmdflags)
            cmd_1b = ["ncks", "--rgr", "xtr_mth=mss_val", f"--vrt_fl={vrt_remap_plev19}", f"""{rgr_dir_vert}/f"{{afile}}" """, f"""{rgr_dir}/f"{{afile}}" """]
            cmd_2 = ["e3sm_to_cmip", "-v", f"{dsd['cmip6var']}", "--freq", f"{freq}", "-u", f"{metadata_file}", "-t", f"{cmor_tables}", "-o", f"{result_dir}", "-i", f"{rgr_dir}"]
        elif the_var_type == "atm_day": 
            cmdflags.extend(["--clm_md=hfs"])
            cmd_1 = ["ncclimo", "-P", "eam", "-j", "1", f"--map={map_file}", f"--start={year_start}", f"--end={year_final}", f"--ypf={ypf}", "--split", f"--caseid={caseid}", "-o", f"{native_out}", "-O", f"{rgr_dir}", "-v", f"{nat_vars}", "-i", f"{native_data}"]
            cmd_1.extend(cmdflags)
            cmd_2 = ["e3sm_to_cmip", "-v", f"{dsd['cmip6var']}", "--freq", f"{freq}", "-u", f"{metadata_file}", "-t", f"{cmor_tables}", "-o", f"{result_dir}", "-i", f"{rgr_dir}"]
        elif the_var_type == "atm_3hr": 
            cmdflags.extend(["--clm_md=hfs"])
            cmd_1 = ["ncclimo", "-P", "eam", "-j", "1", f"--map={map_file}", f"--start={year_start}", f"--end={year_final}", f"--ypf={ypf}", "--split", f"--caseid={caseid}", "-o", f"{native_out}", "-O", f"{rgr_dir}", "-v", f"{nat_vars}", "-i", f"{native_data}"]
            cmd_1.extend(cmdflags)
            cmd_2 = ["e3sm_to_cmip", "-v", f"{dsd['cmip6var']}", "--freq", f"{freq}", "-u", f"{metadata_file}", "-t", f"{cmor_tables}", "-o", f"{result_dir}", "-i", f"{rgr_dir}"]
        elif the_var_type in ["lnd_mon", "lnd_ice_mon"]:            
            cmd_1 = ["ncclimo", "-P", "elm", "-j", "1", "--var_xtr=landfrac", f"--map={map_file}", f"--start={year_start}", f"--end={year_final}", f"--ypf={ypf}", "--split", f"--caseid={caseid}", "-o", f"{native_out}", "-O", f"{rgr_dir}", "-v", f"{nat_vars}", "-i", f"{native_data}"]
            cmd_1.extend(cmdflags)
            cmd_2 = ["e3sm_to_cmip", "-v", f"{dsd['cmip6var']}", "-u", f"{metadata_file}", "-t", f"{cmor_tables}", "-o", f"{result_dir}", "-i", f"{rgr_dir}"]
        elif the_var_type == "mpaso_mon": 
            cmd_2 = ["e3sm_to_cmip", "-v", f"{dsd['cmip6var']}", "-u", f"{metadata_file}", "-t", f"{cmor_tables}", "-o", f"{result_dir}", "-i", f"{native_data}", "-s", "--realm", "mpaso", "--map", f"{map_file}"]
        elif the_var_type == "mpassi_mon": 
            cmd_2 = ["e3sm_to_cmip", "-v", f"{dsd['cmip6var']}", "-u", f"{metadata_file}", "-t", f"{cmor_tables}", "-o", f"{result_dir}", "-i", f"{native_data}", "-s", "--realm", "mpassi", "--map", f"{map_file}"]
        else: 
            log_message("error", f"ERROR: var_type() returned {the_var_type} for cmip dataset_id {dsid}")
            continue

        log_message("info", f"CMD_1:   {cmd_1}")
        log_message("info", f"CMD_1b:  {cmd_1b}")
        log_message("info", f"CMD_2:   {cmd_2}")

        # produce symlinks to native source in native_data
            
        setup_target_symlinks(pargs.run_mode, the_var_type, native_src, native_data, realm, restartf, namefile, region_f)

        # confirm presence of data (symlinks) in native_data
        in_count = dircount(native_data)
        log_message("info", f"NATIVE_SOURCE_COUNT = {in_count} files ({int(in_count/12)} years)")

        #
        # Generate an independent python executable that can be called from this script, or manually
        #
        # IMPORTANT: for large MPAS jobs, this script must be designed to establish the input symlinks by decade,
        # and issue the "cmd2" (e3sm_to_cmip call) for each separate decade, ideally to slurm/srun.
        #
        escript = os.path.join(subscripts, f"{dsid}-gen_CMIP6.py")
        quiet_remove(escript)

        log_message("info", f"   gen escript: {escript}")

        DynaCode_1 = f"""
#!/usr/bin/env python3
# Generating {dsid}:

# This script requires the "datasm" (Data State Machine) system and tools be installed in the environment.

# Import necessary modules
import sys
import os
import subprocess
import fnmatch
import shutil
import time
from datasm.util import get_UTC_TS
from datasm.util import tss
from datasm.util import dirlist
from datasm.util import dircount
from datasm.util import fappend
from datasm.util import force_symlink
from datasm.util import get_srun_status
from datasm.util import force_srun_scancel
from datasm.util import slurm_srun_manager
from datasm.util import slurm_dirs_prep
from datasm.util import setup_logging
from datasm.util import log_message

dsid = "{dsid}"
slogname = "{dsid}.sublog"
slog = os.path.join("{sublogs}", f"{{slogname}}")
print(f"DEBUG: sublog = {{slog}}", flush=True)
if os.path.isfile(slog):
    os.remove(slog)
setup_logging("info", f"{{slog}}")

freq = "{freq}"

run_mode = "{pargs.run_mode}"
native_data = os.path.join("{native_data}")
native_out = os.path.join("{native_out}")
rgr_dir = os.path.join("{rgr_dir}")
rgr_dir_vert = os.path.join("{rgr_dir_vert}")
status_file = os.path.join("{status_file}")
year_start = {year_start}
year_final = {year_final}
ypf = {ypf}
cmdflags = {cmdflags}

the_var_name = "{dsd['cmip6var']}"
the_var_type = "{the_var_type}"

# confirm presence of data (symlinks) in native_data

log_message("info", f"-")
log_message("info", f"======== Generate dataset: {{dsid}} ========")
in_count = dircount(native_data)
log_message("info", f"NATIVE_SOURCE_COUNT = {{in_count}} files ({{int(in_count/12)}} years)")
"""
        fappend(escript, f"{DynaCode_1}")
        log_message("info", "wrote DynaCode_1")

    # ==== Call NCO stuff first if not MPAS ====================================

        if the_var_type not in ["mpaso_mon", "mpassi_mon"]:
            DynaCode_2 = f"""

# Populate a dir_spec dictionary to split input data by year-segments with "slurm_dirs_prep()"

dir_spec = {{}}
dir_spec['method'] = "YPD_LINKS"
dir_spec['srcdir'] = os.path.join("{native_src}")
dir_spec['parent'] = native_data
dir_spec['opt_yr1'] = year_start
dir_spec['opt_ypd'] = int(ypf)
dir_spec['opt_yrs'] = int(year_final) - int(year_start) + 1
dir_spec['extras'] = []
if the_var_type != "atm_mon_fx":
    dir_spec['extdir'] = native_out

segment_specs = slurm_dirs_prep(dir_spec)

ts2 = get_UTC_TS()
log_message("info", f"slurm_dirs_prep() returned {{len(segment_specs)}} segments")

# Create parallel slurm commands to execute the ncclimo calls

cmd_1_group = []

if the_var_type in ['atm_day', 'atm_3hr']:
    cmdflags.extend(["--clm_md=hfs"])

for segspec in segment_specs:
    start_yr = segspec['start']
    final_yr = segspec['final']
    opt_ypd  = segspec['ypd']
    segname  = segspec['segname']
    segpath  = segspec['segpath']
    extpath  = segspec['extpath']
    if the_var_type == "atm_mon_3d":
        cmd_1 = ["ncclimo", "-P", "eam", "-j", "1", f"--map={map_file}", f"--start={{start_yr}}", f"--end={{final_yr}}", f"--ypf={{opt_ypd}}", "--split", f"--caseid={caseid}", "-o", f"{{extpath}}", "-O", f"{rgr_dir_vert}", "-v", f"{nat_vars}", "-i", f"{{segpath}}"]
        cmd_1.extend(cmdflags)
    elif the_var_type != "atm_mon_fx":
        cmd_1 = ["ncclimo", "-P", "eam", "-j", "1", f"--map={map_file}", f"--start={{start_yr}}", f"--end={{final_yr}}", f"--ypf={{opt_ypd}}", "--split", f"--caseid={caseid}", "-o", f"{{extpath}}", "-O", f"{rgr_dir}", "-v", f"{nat_vars}", "-i", f"{{segpath}}"]
        cmd_1.extend(cmdflags)
    else:
        cmd_1 = ["ncremap", f"--map={map_file}", "-v", f"{nat_vars}", "-I", f"{{segpath}}", "-O", f"{rgr_dir}", "--no_stdin"]

    log_message("info", f"cmd_1 = {{cmd_1}}")
    seg_spec = dict()
    seg_spec['segname'] = segname
    seg_spec['seg_cmd'] = cmd_1
    seg_spec['jobname'] = f"nco_{{the_var_name}}_{{segname}}"
    cmd_1_group.append(seg_spec)

passed, failed = slurm_srun_manager(cmd_1_group)
total = passed + failed

log_message("info", f"Slurm NCO (cmd_1) Processing Completed: {{passed}} of {{total}} segments passed ({{failed}} failed.)")

if failed > 0:
    log_message("info", f"ERROR: NCO (cmd_1) Process Fail: dsid = {{dsid}}")
    if run_mode == "WORK":
        ts = f"{{get_UTC_TS()}}"
        fappend(status_file, f"COMM:{{ts}}:POSTPROCESS:DSM_Generate_CMIP6:NCO:Fail")
        fappend(status_file, f"STAT:{{ts}}:POSTPROCESS:DSM_Generate_CMIP6:Fail")
    sys.exit(1)

log_message("info", f"SUCCESS: NCO (cmd_1) Process Pass")
if run_mode == "WORK":
    ts = f"{{get_UTC_TS()}}"
    fappend(status_file, f"COMM:{{ts}}:POSTPROCESS:DSM_Generate_CMIP6:NCO:Pass")
    fappend(status_file, f"STAT:{{ts}}:POSTPROCESS:DSM_Generate_CMIP6:Pass")

"""
            fappend(escript, f"{DynaCode_2}")
            log_message("info", "wrote DynaCode_2")

        if the_var_type == "atm_mon_3d":
            DynaCode_3 = f"""
log_message("info", f"Entered code section for atmos vertical regridding (atm_mon_3D)")

for afile in dirlist(rgr_dir_vert):
    bfile = os.path.basename(afile)
    cmd_1b = ["ncks", "--rgr", "xtr_mth=mss_val", f"--vrt_fl={vrt_remap_plev19}", f"{rgr_dir_vert}/{{bfile}}", f"{rgr_dir}/{{bfile}}"]
    log_message("info", f"LAUNCHING cmd_1b = {{cmd_1b}} file={{bfile}}")
    cmd_result = subprocess.run(cmd_1b, capture_output=True, text=True)
    if cmd_result.returncode != 0:
        log_message("info", f"ERROR: NCO Process (vert regrid) Fail: exit code = {{cmd_result.returncode}}")
        log_message("info", f"STDERR: {{cmd_result.stderr}}")
        log_message("info", f"STDOUT: {{cmd_result.stdout}}")
        if run_mode == "WORK":
            ts = f"{{get_UTC_TS()}}"
            fappend(status_file, f"COMM:{{ts}}:POSTPROCESS:DSM_Generate_CMIP6:NCO(vert_regrid):Fail:return_code={{cmd_result.returncode}}")
            fappend(status_file, f"STAT:{{ts}}:POSTPROCESS:DSM_Generate_CMIP6:Fail:return_code={{cmd_result.returncode}}")
        sys.exit(cmd_result.returncode)
    log_message("info", f"NCO Process (vert regrid) Pass: file={{bfile}}")
    if run_mode == "WORK":
        ts = f"{{get_UTC_TS()}}"
        fappend(status_file, f"COMM:{{ts}}:POSTPROCESS:DSM_Generate_CMIP6:NCO(vert_regrid):Pass")
        fappend(status_file, f"STAT:{{ts}}:POSTPROCESS:DSM_Generate_CMIP6:Pass")
"""
            fappend(escript, f"{DynaCode_3}")
            log_message("info", "wrote DynaCode_3")

    # ==== Call E2C stuff now, if we are still here ====================================
    # if mpaso or mpassi, must loop on ypf-segmentation of "native_data" symlinks
    # if non-MPAS, must loop on ypf-segmentation of the rgr NCO output directory.

        if the_var_type in ["mpaso_mon", "mpassi_mon"]:
            DynaCode_4_for_MPAS = f"""

# Populate a dir_spec dictionary to split input data by year-segments with "slurm_dirs_prep()"

dir_spec = {{}}
dir_spec['method'] = "YPD_LINKS"
dir_spec['srcdir'] = os.path.join("{native_src}")
dir_spec['parent'] = native_data
dir_spec['opt_yr1'] = year_start
dir_spec['opt_ypd'] = int(ypf)
dir_spec['opt_yrs'] = int(year_final) - int(year_start) + 1
dir_spec['extras'] = [ "{restartf}", "{namefile}", "{region_f}" ]

segment_specs = slurm_dirs_prep(dir_spec)

ts2 = get_UTC_TS() 
log_message("info", f"slurm_dirs_prep() returned {{len(segment_specs)}} segment specs")

cmd_2_group = []

for segspec in segment_specs:
    segname = segspec['segname']
    segpath = segspec['segpath']
    cmd_2 = ["e3sm_to_cmip", "-v", f"{{the_var_name}}", "-u", f"{metadata_file}", "-t", f"{cmor_tables}", "-o", f"{result_dir}", "-i", f"{{segpath}}", "-s", "--realm", f"{realm}", "--map", f"{map_file}"]
    log_message("info", f"cmd_2 = {{cmd_2}}")
    seg_cmdspec = dict()
    seg_cmdspec['segname'] = segname
    seg_cmdspec['seg_cmd'] = cmd_2
    seg_cmdspec['jobname'] = f"e2c_{{the_var_name}}_{{segname}}"
    cmd_2_group.append(seg_cmdspec)
"""

            fappend(escript, f"{DynaCode_4_for_MPAS}")
            log_message("info", "wrote DynaCode_4_for_MPAS")

        else:
            DynaCode_4_NON_MPAS = f"""

# Populate a dir_spec dictionary to split input data by year-segments with "slurm_dirs_prep()"

log_message("info", f"Begin NON-MPAS E2C Preparation.  Create slurn-dirs and segment commands")

dir_spec = {{}}
dir_spec['method'] = "PER_FILES"
dir_spec['srcdir'] = rgr_dir
dir_spec['parent'] = rgr_dir
dir_spec['extras'] = []

segment_specs = slurm_dirs_prep(dir_spec)

ts2 = get_UTC_TS() 
log_message("info", f"slurm_dirs_prep() returned {{len(segment_specs)}} segment specs")

cmd_2_group = []

# loop rgr/seg directories on ypf years here

for segspec in segment_specs:
    segname = segspec['segname']
    segpath = segspec['segpath']
    cmd_2 = ["e3sm_to_cmip", "-v", f"{{the_var_name}}", "--freq", f"{{freq}}", "-u", f"{metadata_file}", "-t", f"{cmor_tables}", "-o", f"{result_dir}", "-i", f"{{segpath}}"]
    log_message("info", f"cmd_2 = {{cmd_2}}")
    seg_cmdspec = dict()
    seg_cmdspec['segname'] = segname
    seg_cmdspec['seg_cmd'] = cmd_2
    seg_cmdspec['jobname'] = f"e2c_{{the_var_name}}_{{segname}}"
    cmd_2_group.append(seg_cmdspec)
"""
            fappend(escript, f"{DynaCode_4_NON_MPAS}")
            log_message("info", "wrote DynaCode_4_NON_MPAS")

# This loop of segment processing must submit each segment job to slurm/srun =============================

        DynaCode_5_srun_e2c = f"""

passed, failed = slurm_srun_manager(cmd_2_group)
total = passed + failed

log_message("info", f"Slurm E2C Processing Completed: {{passed}} of {{total}} segments passed ({{failed}} failed.)")

if failed > 0:
    log_message("info", f"ERROR: E2C Process Fail: dsid = {dsid}")
    if run_mode == "WORK":
        ts = f"{{get_UTC_TS()}}"
        fappend(status_file, f"COMM:{{ts}}:POSTPROCESS:DSM_Generate_CMIP6:E2C:Fail")
        fappend(status_file, f"STAT:{{ts}}:POSTPROCESS:DSM_Generate_CMIP6:Fail")
    sys.exit(1)

log_message("info", f"E2C Process Pass")
if run_mode == "WORK":
    ts = f"{{get_UTC_TS()}}"
    fappend(status_file, f"COMM:{{ts}}:POSTPROCESS:DSM_Generate_CMIP6:E2C:Pass")
    fappend(status_file, f"STAT:{{ts}}:POSTPROCESS:DSM_Generate_CMIP6:Pass")

sys.exit(0)
"""
        fappend(escript, f"{DynaCode_5_srun_e2c}")
        log_message("info", "wrote DynaCode_5_srun_e2c")


        DynaCode_6 = f"""
log_message("info", "E2C Process Pass: Cmorizing Successful")

if run_mode == "WORK":
    ts = f"{{get_UTC_TS()}}"
    fappend(status_file, f"COMM:{{ts}}:POSTPROCESS:DSM_Generate_CMIP6:E2C:Pass")
    fappend(status_file, f"STAT:{{ts}}:POSTPROCESS:DSM_Generate_CMIP6:Pass")

sys.exit(0)
"""
        fappend(escript, f"{DynaCode_6}")
        log_message("info", "wrote DynaCode_6")

        log_message("info", f"Produced subordinate script for subprocessing: {escript}")

        if pargs.dryrun:
            continue

    #
    # SECTION:  Subscript Execution ==========================================================
    #

        log_message("info", f"Begin processing dataset_id {dsid} (the_var_type = {the_var_type})")

        if pargs.run_mode == "WORK":
            ts = get_UTC_TS()
            fappend(status_file, f"STAT:{ts}:POSTPROCESS:DSM_Generate_CMIP6:Engaged")


        cmd = ["python", escript]
        cmd_result = subprocess.run(cmd, capture_output=True, text=True)
        if cmd_result.returncode != 0:
            log_message("error", f"Generate CMIP6 CMD FAILED: cmd = {cmd}")
            log_message("error", f"STDERR: {cmd_result.stderr}")
            if pargs.run_mode == "WORK":
                ts = get_UTC_TS()
                fappend(status_file, f"COMM:{ts}:POSTPROCESS:DSM_Generate_CMIP6:Subprocess:Fail:return_code={cmd_result.returncode}")
                fappend(status_file, f"STAT:{ts}:POSTPROCESS:GenerateCMIP6:Fail:return_code={cmd_result.returncode}")
            # continue

    #
    # SECTION:  Product Disposition ==========================================================
    #

        product_dst = ""
        if pargs.run_mode == "WORK":
            facet_path = dsid.replace('.', '/')
            ds_version = get_metadata_file_version(metadata_file)
            product_src = os.path.join(result_dir, facet_path, ds_version)
            product_dst = os.path.join(staging_data, facet_path, ds_version)
            os.makedirs(product_dst, exist_ok=True)
            pattern_src = os.path.join(product_src, "*.nc")

            # DEBUG is there output?
            outtest = dirlist(product_src)
            log_message("info", f"DEBUG: product_src holds the following {len(outtest)} output files:")
            for afile in outtest:
                log_message("info", f"DEBUG: Product facet-path holds {afile}")
            log_message("info", f"DEBUG: Conducting product transfer FROM: {product_src}")
            log_message("info", f"DEBUG: Conducting product transfer INTO: {product_dst}")

            ts = get_UTC_TS()
            if len(outtest) == 0:
                log_message("info", f"ERROR: No output files produced in product directory {product_src}")
                fappend(status_file, f"COMM:{ts}:POSTPROCESS:DSM_Generate_CMIP6:Fail")
            else:    
                glob_move(pattern_src, product_dst)
                log_message("info", f"Completed move of CMIP6 dataset to Staging Data ({product_dst})")
                fappend(status_file, f"COMM:{ts}:POSTPROCESS:DSM_Generate_CMIP6:Pass")

        log_message("info", f"Completed Processing dataset_id: {dsid}")

        cmdr = [f"{run_reporter}", f"{caseid}", f"{dsid}"]
        log_message("info", f"Invoking Report Generator: cmd = {cmdr}")
        fappend(gv_wtf_log, f"Invoking Report Generator: cmd = {cmdr}")
        sleep(5)       
        cmd_result = subprocess.run(cmdr, capture_output=True, text=True)
        ts = get_UTC_TS()
        if cmd_result.returncode != 0:
            fappend(gv_wtf_log, f"{ts}:WARNING: Report Generator FAILED: cmd = {cmdr}")
            fappend(gv_wtf_log, f"{ts}:WARNING: cmd_result.stderr = {cmd_result.stderr}")
        else:
            fappend(gv_wtf_log, f"{ts}:INFO: Report Generator Completed.")

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
    print(f"DEBUG: DGC: mainlog = {mainlog}", flush=True)

    dsid_list = load_file_lines(pargs.input_dsids)

    process_dsids(dsid_list, pargs)
    

# Entry point of the program
if __name__ == "__main__":
    main()

