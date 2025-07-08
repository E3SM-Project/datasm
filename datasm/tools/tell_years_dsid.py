import os, sys
import subprocess
import yaml
import argparse
from argparse import RawTextHelpFormatter
from datasm.util import get_dsspec_year_range


helptext = '''
    Usage:  (python) tell_years_dsid.py -d <dataset_id>
    For the given E3SM dataset_id, report "start_year,end_year" from the dataset_spec.
'''

def assess_args():

    parser = argparse.ArgumentParser(description=helptext, prefix_chars='-', formatter_class=RawTextHelpFormatter)
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')
    required.add_argument('-d', '--dsid', action='store', dest="thedsid", type=str, required=True)

    args = parser.parse_args()

    return args

def main():

    pargs = assess_args()

    dsid = pargs.thedsid
    yr_start,yr_final = get_dsspec_year_range(dsid)
    print(f"{yr_start},{yr_final}")


if __name__ == "__main__":
  sys.exit(main())

