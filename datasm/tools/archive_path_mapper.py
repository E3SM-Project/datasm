import sys, os
import argparse
from argparse import RawTextHelpFormatter
import glob
import shutil
from subprocess import Popen, PIPE, check_output
from datasm.util import get_dsm_paths
import time
from datetime import datetime
import pytz

helptext = '''
    Usage:  archive_path_mapper -a al_listfile [-s sdepfile]

    The archive_path_mapper accepts a file containing one or more Archive_Locator specification line, and

    See: "[ARCHIVE_MANAGEMENT]/Archive_Locator" for archive selection specification lines.

    By default, the archive(s) will be plied against every file-pattern listed in the pattern file
        [ARCHIVE_MANAGEMENT]/Standard_Datatype_Extraction_Patterns

    You can override this to seek only selected patterns by supplying a file of similar format.

    NOTE:  This process requires an environment with zstash v0.4.1 or greater.
'''

dsm_paths = get_dsm_paths()
archmanpath = dsm_paths["ARCHIVE_MANAGEMENT"]
the_SDEP = f"{archmanpath}/Standard_Datatype_Extraction_Patterns"

userroot = dsm_paths["USER_ROOT"]
thisuser = os.getlogin()

WorkDir = f"{userroot}/{thisuser}/Operations/2_ArchiveMapping"

pathsFound = os.path.join(WorkDir,'PathsFound')

holodeck = os.path.join(WorkDir,'Holodeck')
holozst = os.path.join(holodeck,'zstash')


AL_Listfile = ''
thePWD = os.getcwd()

arch_path = ''
x_pattern = ''


def ts():
    return 'TS_' + pytz.utc.localize(datetime.utcnow()).strftime("%Y%m%d_%H%M%S_%f")

out_log = f"{thePWD}/runlog-{ts()}"

def logmsg(msg):
    ts = pytz.utc.localize(datetime.utcnow()).strftime("%Y%m%d_%H%M%S_%f")
    with open(out_log, "a+") as outstream:
        outstream.write(f"{ts}: {msg}\n")

 
def assess_args():
    global AL_Listfile
    global the_SDEP

    # parser = argparse.ArgumentParser(description=helptext, prefix_chars='-')
    parser = argparse.ArgumentParser(description=helptext, prefix_chars='-', formatter_class=RawTextHelpFormatter)
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    required.add_argument('-a', '--al-spec', action='store', dest="AL_Listfile", type=str, required=True)
    optional.add_argument('-s', '--sdep-spec', action='store', dest="SDEP_Pattfile", type=str, required=False)

    args = parser.parse_args()

    AL_Listfile = args.AL_Listfile
    if args.SDEP_Pattfile:
        the_SDEP = args.SDEP_Pattfile

def sort_file_in_place(afile):

    with open(afile) as inF:
        lines = list(line for line in (l.strip() for l in inF) if line) # skip blanks
    lines.sort()
    with open(afile, "w") as outF:
        for line in lines:
            outF.write(f"{line}\n")

def get_al_spec(specline):
    archvals = specline.split(',')
    aspec = {}
    aspec['campa'] = archvals[0]
    aspec['model'] = archvals[1]
    aspec['exper'] = archvals[2]
    aspec['resol'] = archvals[3]
    aspec['ensem'] = archvals[4]
    aspec['apath'] = archvals[5]
    return aspec

# specline = Realm.Grid.Freq,OutType,CorePatt,Campaigns
def get_sdep_spec(specline):
    sdepvals = specline.split(',')
    aspec = {}
    aspec['dtype'] = sdepvals[0].replace('.','_')
    aspec['otype'] = sdepvals[1]
    aspec['spatt'] = sdepvals[2]
    aspec['clist'] = sdepvals[3].split(' ')
    return aspec

disqual = [ 'rest/', 'post/', 'test', 'init', 'run/try', 'run/bench', 'old/run', 'pp/remap', 'a-prime', 'lnd_rerun', 'atm/ncdiff', 'archive/rest', 'fullD', 'photic']
disqual_rst = [ 'post/', 'test', 'run/try', 'run/bench', 'old/run', 'pp/remap', 'a-prime', 'lnd_rerun', 'atm/ncdiff', 'fullD', 'photic']

def recover_filename_elements(filename):
    # convert colon-separated archive_map key to CSV and pipe-coded archive-path to a true path
    am_temp = ':'.join(filename.split(':')[1:])
    old_ret_val = am_temp.replace('|','/').replace(':',',')

    # Must convert
    #   DECK-v1,1_0_LE,ssp370,1deg_atm_60-30km_ocean,ens11,river_native_mon,model-output,<ArchivePath>
    # to
    #   DECK-v1, Proj.Model.Exper.Resol.Realm.Grid.OutType.Freq.Ensem, OutType,<ArchivePath>

    parts = old_ret_val.split(',')
    dtype = parts[5].split('_')
    if len(dtype) > 3:
        dtype[2] = f"{dtype[2]}_{dtype[3]}"

    ret_val = f"{parts[0]},E3SM.{parts[1]}.{parts[2]}.{parts[3]}.{dtype[0]}.{dtype[1]}.{parts[6]}.{dtype[2]}.{parts[4]},{parts[7]}"
    
    return ret_val


def main():

    assess_args()

    zstashversion = check_output(['zstash', 'version']).decode('utf-8').strip()
    # print(f'zstash version: {zstashversion}')

    if zstashversion < "0.4.1":
        logmsg(f"ERROR: ABORTING:  zstash version [{zstashversion}] is not 0.4.1 or greater, or is unavailable")
        sys.exit(1)

    # Clear out PathsFound

    shutil.rmtree(pathsFound,ignore_errors=True)
    os.makedirs(pathsFound)

    # process each archive location

    with open(AL_Listfile) as f:
        al_contents = f.read().split('\n')

    with open(the_SDEP) as f:
        sdep_contents = f.read().split('\n')

    ALE = 0

    al_linelist = [ al_line for al_line in al_contents if al_line[:-1] ]
    for al_line in al_linelist:
        print(f'Processing Archive Locator Entry: {al_line}')
        al_spec = get_al_spec(al_line)
        arch_path = al_spec['apath']
        if arch_path == 'NAV':
            continue
        
        # create SDEP (standard dataset extraction patterns) subset specific to this Campaign
        sdep_list = [ sdep_line for sdep_line in sdep_contents if sdep_line[:-1] ]
        sdep_selected = []      # a list of dictionaries
        for sdep_line in sdep_list:
            sdep_spec = get_sdep_spec(sdep_line)
            if sdep_spec['spatt'] == 'ignore':
                continue
            if al_spec['campa'] not in sdep_spec['clist']:      # al_list campaign MUST be in sdep campaign list
                continue
            logmsg(f"DBG: appending SDEP spec: {sdep_spec}")
            sdep_selected.append(sdep_spec)

        # for adict in sdep_selected:
        #    print(f' DICT = {adict}')

        ALE += 1
        ale_code = f"ALE_{str(ALE).zfill(2)}"

        basetag = ':'.join([al_spec['campa'],al_spec['model'],al_spec['exper'],al_spec['resol'],al_spec['ensem']])

        # prepare the Holodeck
        if os.path.exists(holodeck):
            shutil.rmtree(holodeck,ignore_errors=True)
        os.mkdir(holodeck)
        os.mkdir(holozst)
        os.chdir(holodeck)

        # create the symlink to index.db only
        link = os.path.join(holozst,'index.db')
        targ = os.path.join(arch_path,'index.db')
        os.symlink(targ,link)

        for adict in sdep_selected:
            dstitle = adict['dtype']
            ds_otyp = adict['otype']
            ds_patt = adict['spatt']
            ds_path = arch_path.replace('/','|')
            outfile = ':'.join([ale_code,basetag,dstitle,ds_otyp,ds_path])
            
            # call zstash
            logmsg(f"DBG: Calling zstash ls --hpss=none with pattern: {ds_patt}")
            cmd = ['zstash', 'ls', '--hpss=none', ds_patt]

            try:
                proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
                try:
                    proc_out, proc_err = proc.communicate()
                except:
                    pass
            except:
                pass
            if not proc.returncode == 0:
                logmsg(f"ERROR: zstash returned exitcode {proc.returncode}")
                # sys.exit(proc.returncode)
                continue

            proc_out = proc_out.decode('utf-8')
            proc_err = proc_err.decode('utf-8')
            # print(f'{proc_out}',flush=True)
            if len(proc_err) > 0:
                print(f'{proc_err}',flush=True)

            if len(proc_out) == 0:
                logmsg(f"WARNING: zstash returned no matches")
                continue

            outpath = os.path.join(pathsFound,outfile)
            f = open(outpath,"w")
            f.write(proc_out)
            sort_file_in_place(outpath)

        os.chdir(WorkDir)

    # stage 2 first_last collection

    outF = open("headset_list_first_last", "w")

    for filename in os.listdir(pathsFound):
        thepath = os.path.join('PathsFound',filename)

        dq = disqual
        if 'restart' in thepath:
            dq = disqual_rst 

        qualified = []
        with open(thepath) as f:
            for aline in f:
                if any( aline.startswith(_) for _ in dq ):      # Grrr.  "startswith" acts like "contains"!!!
                    # print(f"APM_DEBUG: disqual: {aline}")
                    continue
                # print(f"APM_DEBUG: qual: {aline}")
                qualified.append(aline)
        if len(qualified):
            qualified.sort()
            am_template = recover_filename_elements(filename)
            outF.write(f'{am_template}\n')
            outF.write(f'    HEADF,{qualified[0]}')
            outF.write(f'    HEADL,{qualified[-1]}')

    print("Stage 2 Completed.  Edit output file \"headset_list_first_last\", rename if desired, and use as input to Stage 3.")


    sys.exit(0)


if __name__ == "__main__":
  sys.exit(main())

