import os, sys, argparse
import yaml
from argparse import RawTextHelpFormatter
from datasm.util import get_dsm_paths


helptext = '''
    Usage:  python list_e3sm_dsids.py  [-d/--dataset-spec <alternate_spec>] (generates all CMIP6 dataset_ids to stdout)
'''

def assess_args():

    parser = argparse.ArgumentParser(description=helptext, prefix_chars='-', formatter_class=RawTextHelpFormatter)
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    optional.add_argument('-d', '--dataset-spec', action='store', dest="dsetspec", type=str, required=False)


    args = parser.parse_args()

    return args

dsm_paths = get_dsm_paths()
resource_path = dsm_paths["STAGING_RESOURCE"]
default_dsetspec = os.path.join(resource_path, 'dataset_spec.yaml')


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

# CMIP6: Project.Activity.Institution.SourceID.Experiment.VariantLabel.RealmFreq.VarName.Grid

def collect_cmip_datasets(dataset_spec):
    for target_project in ["CMIP6", "CMIP6-E3SM-Ext"]:
        for activity_name, activity_val in dataset_spec["project"][target_project].items():
            if activity_name == "test":
                continue
            for institution_id, institution_branch in activity_val.items():
                for version_name, version_value in institution_branch.items():    # version_name is CMIP6 Source_ID
                    for experimentname, experimentvalue in version_value.items():
                        for ensemble in experimentvalue["ens"]:
                            for table_name, table_value in dataset_spec["tables"].items():
                                for variable in table_value:
                                    if ( variable in experimentvalue["except"] or table_name in experimentvalue["except"] or variable == "all"):
                                        continue
                                    dataset_id = f"{target_project}.{activity_name}.{institution_id}.{version_name}.{experimentname}.{ensemble}.{table_name}.{variable}.gr"
                                    yield dataset_id


# E3SM: Project.ModelVersion.Experiment.Resolution.Realm.Grid.OutputType.Freq.Ensemble

def collect_e3sm_datasets(dataset_spec):
    for version in dataset_spec['project']['E3SM']:
        for experiment, experimentinfo in dataset_spec['project']['E3SM'][version].items():
            for ensemble in experimentinfo['ens']:
                for res in experimentinfo['resolution']:
                    for comp in experimentinfo['resolution'][res]:
                        for item in experimentinfo['resolution'][res][comp]:
                            for data_type in item['data_types']:
                                if item.get('except') and data_type in item['except']:
                                    continue
                                dataset_id = f"E3SM.{version}.{experiment}.{res}.{comp}.{item['grid']}.{data_type}.{ensemble}"
                                yield dataset_id


def dsids_from_dataset_spec(dataset_spec):
    cmip6_ids = [x for x in collect_cmip_datasets(dataset_spec)]
    return cmip6_ids

def main():

    pargs = assess_args()

    if pargs.dsetspec:
        ds_spec = load_yaml(pargs.dsetspec)
    else:
        ds_spec = load_yaml(default_dsetspec)

    all_cmip6_dsids = dsids_from_dataset_spec(ds_spec)

    all_cmip6_dsids.sort()

    for dsid in all_cmip6_dsids:
        print(f'{dsid}')

    sys.exit(0)

if __name__ == "__main__":
    sys.exit(main())


