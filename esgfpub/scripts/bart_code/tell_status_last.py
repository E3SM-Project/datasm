import sys, os
import argparse
from argparse import RawTextHelpFormatter
import glob
import shutil
import subprocess
import time


# 
def ts():
    return 'TS_' + datetime.now().strftime('%Y%m%d_%H%M%S')


helptext = '''
    Return the last line(s) of the status file for a dataset indicated by the supplied dataset_id.
'''

gv_stat_root = '/p/user_pub/e3sm/staging/status'

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


def get_statfile_fullpath(dsid):
    statfile = os.path.join(gv_stat_root,dsid + '.status')
    return statfile


def main():

    args = assess_args()
    thedsid = args.thedsid

    sf_path = get_statfile_fullpath(thedsid)
    if not os.path.exists(sf_path):
        print(f'No Status File: {sf_path}')
        sys.exit(0)

    sf_list = loadFileLines(sf_path)

    last = args.n_lines

    sf_len = len(sf_list)

    if not last or (last >= sf_len or last == 0):
        printList('',sf_list)
    else:
        printList('',sf_list[-last:])


    sys.exit(0)

if __name__ == "__main__":
  sys.exit(main())





