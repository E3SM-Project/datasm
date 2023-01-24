import os, sys
import yaml
import argparse
from argparse import RawTextHelpFormatter


helptext = '''
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

# resource_path = '/p/user_pub/e3sm/staging/resource/'
resource_path = '/home/bartoletti1/gitcode/datasm/datasm/resources'

DEFAULT_SPEC_PATH = os.path.join(resource_path, 'dataset_spec.yaml')


def main():

    pargs = assess_args()

    dsid = pargs.thedsid
    dc = dsid.split(".")

    # E3SM:  0=project, 1=model, 2=exper,
    # CMIP:  0=project, 1=activ, 2=sourc, 3=exper,

    with open(DEFAULT_SPEC_PATH, 'r') as instream:
        dataset_spec = yaml.load(instream, Loader=yaml.SafeLoader)

    # for experiment, experimentinfo in dataset_spec['project'][dc[0]][dc[1]].items():
    #     print(f"{experiment}: {experimentinfo}")


    if dc[0] == "E3SM":
        # print(f"DEBUG: headpart = {dc[0]}.{dc[1]}.{dc[2]}")
        the_experiment_record = dataset_spec['project'][dc[0]][dc[1]][dc[2]]
    else:
        # print(f"DEBUG: headpart = {dc[0]}.{dc[1]}.{dc[2]}.{dc[3]}.{dc[4]}")
        the_experiment_record = dataset_spec['project'][dc[0]][dc[1]][dc[2]][dc[3]][dc[4]]

    # print(f"DEBUG: the_experiment_record = {the_experiment_record}")
    print(f"{the_experiment_record['start']},{the_experiment_record['end']}")


if __name__ == "__main__":
  sys.exit(main())

