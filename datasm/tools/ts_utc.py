import sys, os
import argparse
from argparse import RawTextHelpFormatter
from pathlib import Path
import json
import shutil
import time
from datetime import datetime, timezone

# 
def ts():
    return 'TS_' + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")


helptext = '''
'''

def assess_args():

    parser = argparse.ArgumentParser(description=helptext, prefix_chars='-', formatter_class=RawTextHelpFormatter)
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')
    optional.add_argument('-L', '--local', action='store_true', dest="localtz", default=False, required=False)

    args = parser.parse_args()

    return args


def main():

    args = assess_args()

    print(f"{ts()}")

    sys.exit(0)

if __name__ == "__main__":
  sys.exit(main())





