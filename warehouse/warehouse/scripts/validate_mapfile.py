import sys, os
import argparse
from argparse import RawTextHelpFormatter
import glob
import shutil
import subprocess
import time
from datetime import datetime

subcommand = ''
gv_logname = ''

'''
    Usage:  validate_mapfile --data-path version_path --mapfile mapfile_path

    Returns 0 if every file in version path is listed in the mapfile

'''

def parse_args():
    parser = argparse.ArgumentParser(
        description="Ensure every datafile in supplied data_path exists in the given mapfile.")
    parser.add_argument('--data-path', type=str, dest='datapath', required=True, help="source directory of netCDF files to seek in mapfile")
    parser.add_argument('--mapfile', type=str, dest='mapfile', required=True, help="mapfile to be validated")
    return parser.parse_args()


# ======== convenience ========================

def ts(prefix):
    return prefix + datetime.now().strftime('%Y%m%d_%H%M%S')

def loadFileLines(afile):
    retlist = []
    if len(afile):
        with open(afile,"r") as f:
            retlist = f.read().split('\n')
        retlist = [ _ for _ in retlist if _[:-1] ]
    return retlist

def logMessageInit(logtitle):
    global gv_logname
    gv_logname = f'{logtitle}-{ts("")}'
    open(gv_logname, 'a').close()

def logMessage(mtype,message):
    outmessage = f'{ts("TS_")}:{mtype}:{message}\n'
    with open(gv_logname, 'a') as f:
        f.write(outmessage)

# ======== warehouse ========================

do_logging = False

def validate_mapfile(mapfile,srcdir):
    ''' at this point, the srcdir should contain the datafiles (*.nc)
        and the parent dir/.mapfile, so we can do a name-by-name comparison.
        MUST test for each srcdir datafile in mapfile listing.
    '''
    dataset_files = sorted(glob.glob(srcdir + '/*.nc'))
    mapfile_lines = sorted(loadFileLines(mapfile))

    if not len(dataset_files) == len(mapfile_lines):
        if do_logging:
            logMessage('ERROR',f'non-matching count of files and mapfile lines: {srcdir}')
        return False
        
    # MUST assume both lists sort identically - O(n) > O(n^2)
    pairlist = list(zip(dataset_files,mapfile_lines))
    for atup in pairlist:
        if not atup[0] in atup[1]: 
            if do_logging:
                logMessage('ERROR',f'dataset file not listed in mapfile: {mapfile}')
                logMessage('ERROR',f'{atup[0]} not in {atup[1]}')
            return False

    return True


def main():

    args = parse_args()

    if do_logging:
        logMessageInit('rlog_validate_mapfile')

    success = validate_mapfile(args.mapfile,args.datapath)
    if do_logging:
        if success:
            logMessage('STATUS',f'MAPFILE_GEN:Pass')
        else:
            logMessage('STATUS',f'MAPFILE_GEN:Fail:Bad_mapfile')

    sys.exit(success)
 
if __name__ == "__main__":
  sys.exit(main())



