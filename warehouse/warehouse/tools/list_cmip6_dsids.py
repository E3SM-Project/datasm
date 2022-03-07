import os, sys
import yaml

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


def collect_cmip_datasets(dataset_spec):
    for activity_name, activity_val in dataset_spec['project']['CMIP6'].items():
        if activity_name == "test":
            continue
        for version_name, version_value in activity_val.items():
            for experimentname, experimentvalue in version_value.items():
                for ensemble in experimentvalue['ens']:
                    for table_name, table_value in dataset_spec['tables'].items():
                        for variable in table_value:
                            if variable in experimentvalue['except'] or table_name in experimentvalue['except'] or variable == "all":
                                continue
                            dataset_id = f"CMIP6.{activity_name}.E3SM-Project.{version_name}.{experimentname}.{ensemble}.{table_name}.{variable}.gr"
                            yield dataset_id



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


def dsids_from_dataset_spec(dataset_spec_path):
    with open(dataset_spec_path, 'r') as instream:
        dataset_spec = yaml.load(instream, Loader=yaml.SafeLoader)
        cmip6_ids = [x for x in collect_cmip_datasets(dataset_spec)]
        # e3sm_ids = [x for x in collect_e3sm_datasets(dataset_spec)]
        # dataset_ids = cmip6_ids + e3sm_ids

    return cmip6_ids
    # return dataset_ids

all_cmip6_dsids = dsids_from_dataset_spec(DEFAULT_SPEC_PATH)

all_cmip6_dsids.sort()

for dsid in all_cmip6_dsids:
    # print(f'{aline}')
    print(f'{dsid}')
    # print(" ")

