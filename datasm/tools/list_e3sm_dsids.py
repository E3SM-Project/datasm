import os, sys, argparse
import yaml
from argparse import RawTextHelpFormatter


helptext = '''
    Usage:  python list_e3sm_dsids.py  (generates all E3SM dataset_ids to stdout)
'''

def assess_args():

    parser = argparse.ArgumentParser(description=helptext, prefix_chars='-', formatter_class=RawTextHelpFormatter)
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    args = parser.parse_args()

    return args


resource_path = '/p/user_pub/e3sm/staging/resource/'
DEFAULT_SPEC_PATH = os.path.join(resource_path, 'dataset_spec.yaml')

def loadFileLines(afile):
    retlist = []
    if not os.path.exists(afile):
        return retlist
    if len(afile):
        with open(afile,"r") as f:
            retlist = f.read().split('\n')
        retlist = [ _ for _ in retlist if _[:-1] ]
    return retlist


def putFileLines(afile,lines):
    with open(afile, 'w') as f:
        for aline in lines:
            f.write(f'{aline}\n')

def load_yaml(inpath):
    with open(inpath, 'r') as instream:
        in_yaml = yaml.load(instream, Loader=yaml.SafeLoader)
    return in_yaml


# spec hierarchy:  project, model_version, experiment, [ens, (resolution (realm) ), cmip_case, ...]
def collect_e3sm_datasets(dataset_spec):
    for model_version in dataset_spec['project']['E3SM']:
        for experiment, experimentinfo in dataset_spec['project']['E3SM'][model_version].items():
            for ensemble in experimentinfo['ens']:
                for res in experimentinfo['resolution']:
                    for comp in experimentinfo['resolution'][res]:
                        for item in experimentinfo['resolution'][res][comp]:
                            for data_type in item['data_types']:
                                if item.get('except') and data_type in item['except']:
                                    continue
                                dataset_id = f"E3SM.{model_version}.{experiment}.{res}.{comp}.{item['grid']}.{data_type}.{ensemble}"
                                yield dataset_id


def dsids_from_dataset_spec(dataset_spec):
    e3sm_ids = [x for x in collect_e3sm_datasets(dataset_spec)]
    return e3sm_ids

def expand_dataset_spec(dataset_spec):
    global gv_outfile

    Extn_Table = dataset_spec['CASE_EXTENSIONS']

    for model_version in dataset_spec['project']['E3SM']:
        for experiment, experimentinfo in dataset_spec['project']['E3SM'][model_version].items():
            extn_id = experimentinfo['resolution']
            if not extn_id in Extn_Table:
                print(f"ERROR: extension ID {extn_id} not found in extension table for {model_version} {experiment}")
                sys.exit(1)
            experimentinfo['resolution'] = Extn_Table[extn_id]


def main():

    assess_args()

    ds_spec = load_yaml(DEFAULT_SPEC_PATH)

    if 'CASE_EXTENSIONS' in ds_spec.keys():
        expand_dataset_spec(ds_spec)

    all_e3sm_dsids = dsids_from_dataset_spec(ds_spec)

    all_e3sm_dsids.sort()

    for dsid in all_e3sm_dsids:
        print(f'{dsid}')

    sys.exit(0)

if __name__ == "__main__":
    sys.exit(main())


