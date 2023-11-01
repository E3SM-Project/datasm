import sys, os
import argparse
from argparse import RawTextHelpFormatter
import glob
import shutil
from datasm.util import get_dsm_paths
from subprocess import Popen, PIPE, check_output
import time
from datetime import datetime
import pytz
from pathlib import Path

gv_logname = ''

dsm_paths = get_dsm_paths()
staging = dsm_paths["DSM_STAGING"]
archman = dsm_paths["ARCHIVE_MANAGEMENT"]
gv_WH_root = dsm_paths["STAGING_DATA"]
gv_PUB_root = dsm_paths["PUBLICATION_DATA"]
gv_stat_root = dsm_paths["STAGING_STATUS"]

gv_holospace = f"{staging}/holospace"
gv_input_dir = f"{archman}/extraction_requests_pending"
gv_output_dir = f"{archman}/extraction_requests_processsed"

helptext = '''

    usage:  nohup python archive_extraction_service.py [-h/--help] &

    The archive_extraction_service checks for the oldest extraction-request file in 

        [ARCHIVE_MANAGEMENT]/extraction_requests_pending/

    Each request file must have the name "extract_request-<dsid>", and must contain one or more lines
    from the Archive_Map ([ARCHIVE_MANAGEMENT]/Archive_Map) sufficient to fully cover the
    intended dataset extraction.  Some datasets are spread across multiple archive paths.

    A separate utility, archive_map_to_dsid, can be supplied a list of Archive_Map lines, and
    will produce as output the dsid-named files, each containing the Archive_Map entries that
    correspond to the given dataset id.

'''

# ======== convenience ========================

def ts(prefix):
    return prefix + pytz.utc.localize(datetime.utcnow()).strftime("%Y%m%d_%H%M%S_%f")

def assess_args():

    parser = argparse.ArgumentParser(description=helptext, prefix_chars='-', formatter_class=RawTextHelpFormatter)
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    args = parser.parse_args()

    return True


def print_list(prefix, items):
    for x in items:
        print(f'{prefix}{x}')

def print_file_list(outfile, items):
    with open(outfile, 'w') as outstream:
        for x in items:
            outstream.write(f"{x}\n")

def file_append_line(afile,aline):
    outline = aline
    if not aline[-1] == '\n':
        outline = f'{aline}\n'
    with open(afile, 'a') as f:
        f.write(outline)

def logMessageInit(logtitle):
    global gv_logname

    curp = os.getcwd()
    gv_logname = f'{logtitle}-{ts("")}'
    gv_logname = os.path.join(curp,gv_logname)
    open(gv_logname, 'a').close()

def logMessage(mtype,message):
    outmessage = f'{ts("TS_")}:{mtype}:{message}\n'
    with open(gv_logname, 'a') as f:
        f.write(outmessage)

def load_file_lines(file_path):
    if not file_path:
        return list()
    # file_path = Path(file_path)
    if not os.path.exists(file_path):
        logMessage('ERROR',f'File {file_path} either does not exist or is not a regular file')
    with open(file_path, "r") as instream:
        retlist = [[i for i in x.split('\n') if i].pop() for x in instream.readlines() if x[:-1]]
    return retlist

# ======== warehouse ========================

def get_archspec(archline):

    archspec = dict()
    if len( archline.split(',') ) != 4:
        return None

    archspec['campaign'] = archline.split(',')[0]
    archspec['dsid'] = archline.split(',')[1]
    archspec['archpath'] = archline.split(',')[2]
    archspec['filepatt'] = archline.split(',')[3]

    return archspec

def get_dsid_via_archline(archline):

    dsid = archline.split(',')[1]
    return dsid

def get_warehouse_path_via_dsid(dsid):
    path_body = dsid.replace('.',os.sep)
    enspath = os.path.join(gv_WH_root,path_body)
    return enspath

def realm_longname(realmcode):
    ret = realmcode
    if realmcode == 'atm':
        ret = 'atmos'
    elif realmcode == 'lnd':
        ret = 'land'
    elif realmcode == 'ocn':
        ret = 'ocean'

    return ret
                   
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

#            p_ver = maxversion(v_list)
#            if p_ver != "vNONE":

def setStatus(statfile,parent,statspec):
    tsval = ts('')
    statline = f'STAT:{tsval}:{parent}:{statspec}\n'
    with open(statfile, 'a') as statf:
        statf.write(statline)

# return path to unique status file (warehouse or publication)
# create in warehouse if not found

def ensureStatusFile(dsid):
    statfile = os.path.join(gv_stat_root,dsid + '.status')
    if not os.path.exists(statfile):
        open(statfile,"w+").close()
        statid=f"DATASETID={dsid}\n"
        with open(statfile, 'a') as statf:
            statf.write(statid)
        setStatus(statfile,'DATASM','EXTRACTION:Ready')
        setStatus(statfile,'DATASM','VALIDATION:Unblocked:')
        setStatus(statfile,'DATASM','POSTPROCESS:Unblocked:')
        setStatus(statfile,'DATASM','PUBLICATION:Blocked:')
        setStatus(statfile,'DATASM','PUBLICATION:Unapproved:')
        setStatus(statfile,'DATASM','CLEANUP:Blocked:')
        logMessage('INFO',f'ARCHIVE_EXTRACTION_SERVICE:Created new status file for dataset:{dsid}')
        time.sleep(5)

    return statfile

def ensureDatasetPath(ens_path):
    if not os.path.exists(ens_path):
        os.makedirs(ens_path,exist_ok=True)
        os.chmod(ens_path,0o775)

def ensureDestinationVersion(ens_path):
    if not os.path.exists(ens_path):
        return Path( os.path.join(ens_path,"v0") )
    v_list = next(os.walk(ens_path))[1]
    p_ver = maxversion(v_list)
    if p_ver == "vNONE":   
        return Path( os.path.join(ens_path,"v0") )
    next_num = 1 + int(p_ver[1:])
    next_ver = f"v{next_num}"
    return Path( os.path.join(ens_path,next_ver) )

def collision_free_name(apath, abase):
    ''' assuming we must protect a file's extension "filename.ext"
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



# Must ensure we have a DSID name for the dataset status file, before warehouse facet directory exists.
# Must test for existence of facet dest, if augmenting.  May create to 0_extraction/init_status_files/, move later

verbose=True

def main():

    assess_args()
    logMessageInit('runlog_archive_extraction_service')

    zstashversion = check_output(['zstash', 'version']).decode('utf-8').strip()
    # print(f'zstash version: {zstashversion}')

    zstash_main_version = zstashversion[0:2]
    if zstash_main_version == 'v0':
        logMessage("ERROR",f"ARCHIVE_EXTRACTION_SERVICE: zstash version ({zstashversion}) must be 1.0.0 or greater, or is unavailable")
        sys.exit(1)

    logMessage("INFO",f"ARCHIVE_EXTRACTION_SERVICE:Startup:zstash version = {zstashversion}")

    # The outer request loop:
    while True:
        req_files = glob.glob(gv_input_dir + '/*')
        req_files.sort(key=os.path.getmtime)
        if not len(req_files):
            # print(f'DEBUG: sleeping 60')
            time.sleep(60)
            continue


        request_file = req_files[0]  # or req_files[-1], ensure oldest is selected
        request_spec = load_file_lines(request_file) # list of Archive_Map lines for one dataset

        # move request file to gv_output_dir
        request_file_done = os.path.join(gv_output_dir,os.path.basename(request_file))
        if os.path.exists(request_file_done):
            os.remove(request_file_done)
        shutil.move(request_file,gv_output_dir)

        # possible multiple lines for a single dataset extraction request
        # The Inner Loop
        statfile = ''
        newstat = False
        for am_spec_line in request_spec:

            # BECOME SETUP:  Ensure dsid, paths and status file are ready:

            logMessage('INFO',f'ARCHIVE_EXTRACTION_SERVICE:Conducting Setup for extraction request:{am_spec_line}')
            arch_spec = get_archspec(am_spec_line)
            if arch_spec == None:
                logMessage('ERROR',f'ARCHIVE_EXTRACTION_SERVICE: bad am_spec_line: {am_spec_line}')
                continue

            dsid = arch_spec['dsid']

            statfile = ensureStatusFile(dsid)
            ens_path = get_warehouse_path_via_dsid(dsid)        # intended warehouse dataset ensemble path

            # logMessage('DEBUG',f'Preparing to create ens_path {ens_path}')

            ensureDatasetPath(ens_path)

            setStatus(statfile,'DATASM','EXTRACTION:Engaged')
            setStatus(statfile,'EXTRACTION','SETUP:Engaged')

            # negotiate for "best dest_path" here in case existing will interfere:
            dest_path = ensureDestinationVersion(ens_path)


            setStatus(statfile,'EXTRACTION','SETUP:Pass')
            setStatus(statfile,'EXTRACTION','ZSTASH:Ready')
            

            # BECOME ZSTASH:  Create Holodeck and use zstash to extract files:

            logMessage('INFO',f'ARCHIVE_EXTRACTION_SERVICE:Preparing zstash holodeck:{am_spec_line}')

            tm_start = time.time()

            arch_path = arch_spec['archpath']
            arch_patt = arch_spec['filepatt']
            holodeck = os.path.join(gv_holospace,"holodeck-" + ts('') )
            holozst = os.path.join(holodeck,'zstash')

            if not os.path.exists(arch_path):
                logMessage('ERROR',f'ARCHIVE_EXTRACTION_SERVICE: no path {arch_path} found for am_spec_line {am_spec_line}')
                continue

            # logMessage('DEBUG',f'Preparing zstash holodeck: {holodeck}')

            # create holodeck and symlinks
            parentdir = os.getcwd()
            os.mkdir(holodeck)
            os.mkdir(holozst)
            os.chdir(holodeck) # new PWD
            for item in os.scandir(arch_path):
                base = item.path.split(os.sep)[-1]      # get archive item basename
                link = os.path.join(holozst,base)       # create full link name
                os.symlink(item.path,link)

            logMessage('INFO',f'ARCHIVE_EXTRACTION_SERVICE:Executing zstash extraction:{am_spec_line}')
            setStatus(statfile,'EXTRACTION','ZSTASH:Engaged')

            # call zstash and wait for return
            if verbose:
                cmd = ['zstash', 'extract', '--hpss=none', '--verbose', arch_patt]
            else:
                cmd = ['zstash', 'extract', '--hpss=none', arch_patt]
            proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
            proc_out, proc_err = proc.communicate()
            proc_out = proc_out.decode('utf-8')
            proc_err = proc_err.decode('utf-8')

            if not proc.returncode == 0:
                logMessage('ERROR',f'zstash returned exitcode {proc.returncode}')
                if verbose:
                    logMessage('ERROR',f'(verbose) proc_out: {proc_out}')
                logMessage('ERROR',f'(verbose) proc_err: {proc_err}')
                setStatus(statfile,'EXTRACTION',f'ZSTASH:Fail:exitcode={proc.returncode}')
                setStatus(statfile,'DATASM',f'EXTRACTION:Fail')
                os.chdir(parentdir)
                shutil.rmtree(holodeck,ignore_errors=True)
                time.sleep(5)
                continue
            
            if verbose:
                logMessage('INFO',f"(verbose) proc_out: {proc_out}")    
            logMessage('INFO','ARCHIVE_EXTRACTION_SERVICE:zstash completed.')
            setStatus(statfile,'EXTRACTION','ZSTASH:Pass')
            setStatus(statfile,'DATASM',f'EXTRACTION:Pass')

            # CONFIRM files extracted

            extracted_files = glob.glob(arch_patt)
            logMessage("INFO",f"Extracted {len(extracted_files)} files with arch_patt: {arch_patt}")
            if len(extracted_files) == 0:
                logMessage("ERROR",f"Failure to extract files: {dsid}")
                setStatus(statfile,'EXTRACTION','ZSTASH:Fail')
                setStatus(statfile,'DATASM',f'EXTRACTION:Fail')
                continue

            # BECOME TRANSFER:  move Holodeck files to warehouse destination path:

            logMessage('INFO',f'ARCHIVE_EXTRACTION_SERVICE:Begin file transfer to warehouse: {dest_path}')
            setStatus(statfile,'EXTRACTION','TRANSFER:Ready')

            os.makedirs(dest_path,exist_ok=True)
            os.chmod(dest_path,0o775)

            setStatus(statfile,'EXTRACTION','TRANSFER:Engaged')
            fcount = 0
            for datafile in extracted_files:
                bname = os.path.basename(datafile)
                cname = collision_free_name(dest_path,bname)
                dst = os.path.join(dest_path,cname)

                moved=True
                try:
                    shutil.move(datafile,dst)    # move all files, including .status and (if exists) .
                except shutil.Error:
                    logMessage('WARNING',f'shutil - cannot move file: {dst}')
                    moved=False
                try:
                    os.chmod(dst,0o664)
                except:
                    logMessage('WARNING',f'cannot chmod file: {dst}')

                if moved:
                    fcount += 1

            
            tm_final = time.time()
            ET = tm_final - tm_start
            logMessage('INFO',f'ARCHIVE_EXTRACTION_SERVICE:Completed file transfer to warehouse: dsid={dsid}: filecount = {fcount}, ET = {ET}')
            logMessage('INFO', ' ');
            setStatus(statfile,'EXTRACTION',f'TRANSFER:Pass:dstdir=v0,filecount={fcount}')
            os.chdir(parentdir)
            shutil.rmtree(holodeck,ignore_errors=True)

            setStatus(statfile,'DATASM',f'EXTRACTION:Pass:dstdir=v0,filecount={fcount}')

            setStatus(statfile,'DATASM','VALIDATION:Ready')

        time.sleep(5)


if __name__ == "__main__":
  sys.exit(main())


