import sys, os
import argparse
from argparse import RawTextHelpFormatter
import glob
import shutil
import subprocess
import time
import pytz

# 
def ts():
    return 'TS_' + pytz.utc.localize(datetime.utcnow()).strftime("%Y%m%d_%H%M%S_%f")


helptext = '''
    Return the last line(s) of the status file for a dataset indicated by the supplied dataset_id.
'''

gv_stat_root = '/p/user_pub/e3sm/staging/status'
gv_stat_root_ext = '/p/user_pub/e3sm/staging/status_ext'

def assess_args():

    parser = argparse.ArgumentParser(description=helptext, prefix_chars='-', formatter_class=RawTextHelpFormatter)
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')
    required.add_argument('-d', '--dataset_id', action='store', dest="thedsid", type=str, required=True)
    optional.add_argument('-n', '--n-lines', action='store', dest="n_lines", type=int, help='report last n lines of the file, 0=all', required=False)

    args = parser.parse_args()

    if not args.n_lines:
        args.nlines = 1

    return args

def ts_format(etime):
    return time.strftime('%Y%m%d.%H%M%S', time.localtime(etime))

# Generic Convenience Functions =============================

def loadFileLines(afile):
    retlist = []
    if not len(afile) or not os.path.exists(afile):
        return retlist
    with open(afile,"r") as f:
        retlist = f.read().split('\n')
    retlist = [ _ for _ in retlist if _[:-1] ]
    return retlist

def printList(prefix,alist):
    for _ in alist:
        print(f'{prefix}{_}')

def is_dsid_external(dsid)
    project = dsid.split(".")[0]
    if dsid.split(".")[0] == "E3SM":  # project
        return False
    if dsid.split(".")[0] == "CMIP6": # project
        if dsid.split(".")[2] == "E3SM-Project":  # institution_id
            return False
    return True

def get_statfile_path(dsid):
    sp_root = gv_stat_root
    if is_dsid_external(dsid):
        sp_root = gv_stat_root_ext
    s_path = os.path.join(gv_stat_root,dsid + '.status')
    if os.path.exists(s_path):
        return s_path
    return ""

# sf_status = get_sf_laststat(epath)

def get_sf_laststat(dsid):
    sf_path = get_statfile_path(dsid)
    if sf_path == '':
        return ':NO_STATUS_FILE_PATH'
    sf_rawlist = loadFileLines(sf_path)
    sf_list = list()
    for aline in sf_rawlist:
        if aline.split(':')[0] != "STAT":
            continue
        sf_list.append(aline)
    if len(sf_list) == 0:
        return ':EMPTY_STATUS_FILE'
    sf_last = sf_list[-1]
    last_stat = ':'.join(sf_last.split(':')[1:])
    return last_stat

def main():

    args = assess_args()
    thedsid = args.thedsid

    retval = get_sf_laststat(thedsid)

    print(f"{retval}")

    sys.exit(0)

if __name__ == "__main__":
  sys.exit(main())





