import os
import sys
import argparse
import yaml
from argparse import RawTextHelpFormatter


helptext = '''
    Usage:  (python) tell_years_dsid.py -d <dataset_id> [-s <path_to_dataset_spec.yaml>]
    For the given E3SM dataset_id, report "start_year,end_year" from the dataset_spec.
'''

def assess_args():

    parser = argparse.ArgumentParser(description=helptext, prefix_chars='-', formatter_class=RawTextHelpFormatter)
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')
    required.add_argument('-d', '--dsid', action='store', dest="thedsid", type=str, required=True)
    optional.add_argument('-s', '--spec', action='store', dest="thespec", type=str, required=False)

    args = parser.parse_args()

    return args

resource_path = '/p/user_pub/e3sm/staging/resource/'

DEFAULT_SPEC_PATH = os.path.join(resource_path, 'dataset_spec.yaml')


def main():

    pargs = assess_args()

    dsid = pargs.thedsid
    dc = dsid.split(".")        # E3SM:  0=project, 1=model, 2=exper, 3=resol, 4=realm, 5=

    if pargs.thespec:
        if os.path.exists(pargs.thespec):
            with open(pargs.thespec, 'r') as instream:
                dataset_spec = yaml.load(instream, Loader=yaml.SafeLoader)
        else:
            print(f"ERROR: cannot access supplied dataset_spec file: {pargs.thespec}")
            sys.exit(1)
        
    else:
        with open(DEFAULT_SPEC_PATH, 'r') as instream:
            dataset_spec = yaml.load(instream, Loader=yaml.SafeLoader)

    # for experiment, experimentinfo in dataset_spec['project'][dc[0]][dc[1]].items():
    #     print(f"{experiment}: {experimentinfo}")

    the_experiment_record = dataset_spec['project'][dc[0]][dc[1]][dc[2]]
    print(f"{the_experiment_record['start']},{the_experiment_record['end']}")


if __name__ == "__main__":
  sys.exit(main())

