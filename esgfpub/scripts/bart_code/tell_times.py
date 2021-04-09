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
    (write your formatted help text here . . .)
'''

def assess_args():

    parser = argparse.ArgumentParser(description=helptext, prefix_chars='-', formatter_class=RawTextHelpFormatter)
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')
    required.add_argument('-d', '--directory', action='store', dest="thedir", type=str, required=True)
    optional.add_argument('--content', action='store_true', required=False)

    args = parser.parse_args()

    return args

def ts_format(etime):
    return time.strftime('%Y%m%d.%H%M%S', time.localtime(etime))

def main():

    args = assess_args()
    thedir = args.thedir
    content = args.content

    a_time = ts_format(os.stat(thedir).st_atime)
    m_time = ts_format(os.stat(thedir).st_mtime)
    c_time = ts_format(os.stat(thedir).st_ctime)

    print(f'A_time={a_time},M_time={m_time},C_time={c_time}:',end='')
    oldtime = [a_time,m_time,c_time]
    oldtime.sort()
    print(f'OLDEST={oldtime[0]}')

    if not content:
        sys.exit(0)

    filelist = []
    for root, dirs, files in os.walk(thedir):      # file in filelist
        for afile in files:
            filelist.append(os.path.join(thedir,afile))

    for afile in filelist:
        a_time = ts_format(os.stat(afile).st_atime)
        m_time = ts_format(os.stat(afile).st_mtime)
        c_time = ts_format(os.stat(afile).st_ctime)
        print(f'A_time={a_time},M_time={m_time},C_time={c_time}:',end='')
        oldtime = [a_time,m_time,c_time]
        oldtime.sort()
        print(f'OLDEST={oldtime[0]}:{afile}')

    sys.exit(0)

if __name__ == "__main__":
  sys.exit(main())





