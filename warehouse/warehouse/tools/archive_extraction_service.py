import sys, os
import argparse
from argparse import RawTextHelpFormatter
import glob
import shutil
import subprocess
from subprocess import Popen, PIPE, check_output
import time
from datetime import datetime
import pytz
from pathlib import Path

gv_logname = ''
gv_holospace = '/p/user_pub/e3sm/staging/holospace'

gv_WH_root = '/p/user_pub/e3sm/warehouse'
gv_PUB_root = '/p/user_pub/work'
gv_input_dir = '/p/user_pub/e3sm/archive/.extraction_requests_pending'
gv_output_dir = '/p/user_pub/e3sm/archive/.extraction_requests_processed'
gv_stat_root = '/p/user_pub/e3sm/staging/status'

helptext = '''

    usage:  nohup python archive_extraction_service.py [-h/--help] [-c/--config jobset_configfile] &

    The default jobset config file is /p/user_pub/e3sm/archive/.cfg/jobset_config.  It contains

        project=<project>       (default is E3SM)
        pubversion=<vers>       (default is v0)
        pub_root=<path>         (default is /p/user_pub/e3sm/warehouse/E3SM)
        overwrite=<True|False>  (default is True, allows adding files to a non-empty destination.)

    These values must apply to every experiment/archive line pulled from the extraction requests directory.

    The archive_extraction_service checks for the oldest extraction-request file in 

        /p/user_pub/e3sm/archive/.extraction_requests_pending/

    Each request file must have the name "extract_request-<dsid>", and must contain one or more lines
    from the Archive_Map (/p/user_pub/e3sm/archive/.cfg/Archive_Map) sufficient to fully cover the
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
    optional.add_argument('-c', '--config', action='store', dest="jobset_config", type=str, required=False)

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

def parse_jobset_config(configfile):
    speclist = load_file_lines(configfile)
    jobset = dict()
    for _ in speclist:
        pair = _.split('=')    # each a list with two elements
        jobset[ pair[0] ] = pair[1]
        # print(f'  jobset[ {pair[0]} ] = {pair[1]
    return jobset


def get_archspec(archline):
    archvals = archline.split(',')
    archspec = {}
    archspec['campa'] = archvals[0]
    archspec['model'] = archvals[1]
    archspec['exper'] = archvals[2]
    archspec['resol'] = archvals[3]
    archspec['ensem'] = archvals[4]
    archspec['dstyp'] = archvals[5]
    archspec['apath'] = archvals[6]
    archspec['apatt'] = archvals[7]

    return archspec

def get_dsid_via_archline(archline):

    archspec = get_archspec(archline)

    if len(archspec['dstyp'].split('_')) == 3:
        realmcode, grid, freq = archspec['dstyp'].split('_')
    else:
        realmcode, grid, freq1, freq2 = archspec['dstyp'].split('_')
        freq = ('_').join([freq1,freq2])

    realm = realm_longname(realmcode)
    if grid == 'nat':
        grid = 'native'

    # note: model-output assumption on archive only
    dsid = '.'.join(['E3SM', \
                    archspec['model'], \
                    archspec['exper'], \
                    archspec['resol'], \
                    realm, \
                    grid, \
                    'model-output', \
                    freq, \
                    archspec['ensem']])

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
        statid=f"DATASETID={dsid}"
        with open(statfile, 'a') as statf:
            statf.write(statid)
        setStatus(statfile,'WAREHOUSE','EXTRACTION:Ready')
        setStatus(statfile,'WAREHOUSE','VALIDATION:Unblocked:')
        setStatus(statfile,'WAREHOUSE','POSTPROCESS:Unblocked:')
        setStatus(statfile,'WAREHOUSE','PUBLICATION:Blocked:')
        setStatus(statfile,'WAREHOUSE','PUBLICATION:Unapproved:')
        setStatus(statfile,'WAREHOUSE','CLEANUP:Blocked:')
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

def main():

    assess_args()
    logMessageInit('runlog_archive_extraction_service')

    zstashversion = check_output(['zstash', 'version']).decode('utf-8').strip()
    # print(f'zstash version: {zstashversion}')

    if not (zstashversion == 'v0.4.1' or zstashversion == 'v0.4.2' or zstashversion == 'v1.0.0' or zstashversion == 'v1.1.0' ):
        logMessage('ERROR',f'ARCHIVE_EXTRACTION_SERVICE: zstash version ({zstashversion})is not 0.4.1 or greater, or is unavailable')
        sys.exit(1)

    logMessage('INFO',f'ARCHIVE_EXTRACTION_SERVICE:Startup:zstash version = {zstashversion}')

    # The outer request loop:
    while True:
        req_files = glob.glob(gv_input_dir + '/*')
        req_files.sort(key=os.path.getmtime)
        if not len(req_files):
            # print(f'DEBUG: sleeping 60')
            time.sleep(60)
            continue


        request_file = req_files[0]  # or req_files[-1], ensure oldest is selected
        dataset_spec = load_file_lines(request_file) # list of Archive_Map lines for one dataset

        # move request file to gv_output_dir
        request_file_done = os.path.join(gv_output_dir,os.path.basename(request_file))
        if os.path.exists(request_file_done):
            os.remove(request_file_done)
        shutil.move(request_file,gv_output_dir)

        # possible multiple lines for a single dataset extraction request
        # The Inner Loop
        statfile = ''
        newstat = False
        for am_spec_line in dataset_spec:

            # BECOME SETUP:  Ensure dsidi, paths and status file are ready:

            logMessage('INFO',f'ARCHIVE_EXTRACTION_SERVICE:Conducting Setup for extraction request:{am_spec_line}')
            arch_spec = get_archspec(am_spec_line)
            dsid = get_dsid_via_archline(am_spec_line)
            statfile = ensureStatusFile(dsid)
            ens_path = get_warehouse_path_via_dsid(dsid)        # intended warehouse dataset ensemble path

            # logMessage('DEBUG',f'Preparing to create ens_path {ens_path}')

            ensureDatasetPath(ens_path)

            setStatus(statfile,'WAREHOUSE','EXTRACTION:Engaged')
            setStatus(statfile,'EXTRACTION','SETUP:Engaged')

            # negotiate for "best dest_path" here in case existing will interfere:
            dest_path = ensureDestinationVersion(ens_path)


            setStatus(statfile,'EXTRACTION','SETUP:Pass')
            setStatus(statfile,'EXTRACTION','ZSTASH:Ready')
            

            # BECOME ZSTASH:  Create Holodeck and use zstash to extract files:

            logMessage('INFO',f'ARCHIVE_EXTRACTION_SERVICE:Preparing zstash holodeck:{am_spec_line}')

            tm_start = time.time()

            arch_path = arch_spec['apath']
            arch_patt = arch_spec['apatt']
            holodeck = os.path.join(gv_holospace,"holodeck-" + ts('') )
            holozst = os.path.join(holodeck,'zstash')

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
            cmd = ['zstash', 'extract', '--hpss=none', arch_patt]
            proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
            proc_out, proc_err = proc.communicate()

            if not proc.returncode == 0:
                logMessage('ERROR',f'zstash returned exitcode {proc.returncode}')
                setStatus(statfile,'EXTRACTION',f'ZSTASH:Fail:exitcode={proc.returncode}')
                os.chdir(parentdir)
                shutil.rmtree(holodeck,ignore_errors=True)
                time.sleep(5)
                continue
                
            logMessage('INFO','ARCHIVE_EXTRACTION_SERVICE:zstash completed.')
            setStatus(statfile,'EXTRACTION','ZSTASH:Pass')

            proc_out = proc_out.decode('utf-8')
            proc_err = proc_err.decode('utf-8')
            print(f'{proc_out}',flush=True)
            print(f'{proc_err}',flush=True)


            # BECOME TRANSFER:  move Holodeck files to warehouse destination path:

            logMessage('INFO',f'ARCHIVE_EXTRACTION_SERVICE:Begin file transfer to warehouse: {dest_path}')
            setStatus(statfile,'EXTRACTION','TRANSFER:Ready')

            os.makedirs(dest_path,exist_ok=True)
            os.chmod(dest_path,0o775)

            setStatus(statfile,'EXTRACTION','TRANSFER:Engaged')
            fcount = 0
            for datafile in glob.glob(arch_patt):
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
            logMessage('INFO',f'ARCHIVE_EXTRACTION_SERVICE:Completed file transfer to warehouse: filecount = {fcount}, ET = {ET}')
            setStatus(statfile,'EXTRACTION',f'TRANSFER:Pass:dstdir=v0,filecount={fcount}')
            os.chdir(parentdir)
            shutil.rmtree(holodeck,ignore_errors=True)

            setStatus(statfile,'WAREHOUSE',f'EXTRACTION:Pass:dstdir=v0,filecount={fcount}')


        setStatus(statfile,'WAREHOUSE','VALIDATION:Ready')

        time.sleep(5)


if __name__ == "__main__":
  sys.exit(main())


