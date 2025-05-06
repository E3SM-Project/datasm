import os, sys, argparse
import yaml
from argparse import RawTextHelpFormatter


helptext = '''
    Usage:  python dsspec_contract.py  -i dataset_spec_expanded.yaml -o dataset_spec_contracted.yaml

    Reads in the E3SM dataset_spec (example: [STAGING_RESOURCE]/dataset_spec.yaml).
    (Assumes the dataset_spec is in ordinary expanded form, and has no "CASE_EXTENSIONS:" section.)
    For each Project=E3SM Model_Version and Experiment, it locates the branch(es) labeled "resolution",
    and determines the unique set of these, to be labeled "CASE_EXTENSION_01", "CASE_EXTENSION_02", etc.
    These extensions are made a separate "CASE_EXTENSIONS" table in the contracted dataset_spec, and
    for each project:E3SM:model_version:experiment, the appropriate extn_id "CASE_EXTENSION_nn" replaces
    the actual extension.
'''

def assess_args():

    parser = argparse.ArgumentParser(description=helptext, prefix_chars='-', formatter_class=RawTextHelpFormatter)
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    required.add_argument('-i', '--infile', action='store', dest="in_dsspec", type=str, required=True)
    optional.add_argument('-o', '--outfile', action='store', dest="out_dsspec", type=str, required=True)

    args = parser.parse_args()

    return args


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

gv_outpath="."

def prindent(astr,indent,outfile):
    spaces="                                                                                "
    outstr=f"{spaces[:indent]}{astr}"
    dest=f"{gv_outpath}/{outfile}"
    with open(dest, 'a+') as f:
        f.write(f'{outstr}\n')

def dict_reorder(item):
    return {k: dict_reorder(v) if isinstance(v, dict) else v for k, v in sorted(item.items())}

gv_indent = 0
gv_dosort = False

# recurse on dict or list ...

def yaml_write(adict, tsize, outfile):
    global gv_indent
    global gv_dosort

    # Grrrr.
    mod_list = [ '1_0', '1_1', '1_1_ECA', '1_2', '1_2_1', '1_3', '2_0' ]

    if gv_dosort:
        adict = dict_reorder(adict)

    for thing in adict:
        atyp = type(adict[thing])
        if atyp != dict and atyp != list:
            if str(thing) in mod_list:
                xstr = f"'{str(thing)}': {adict[thing]}"
            else:
                xstr = f"{thing}: {adict[thing]}"
            prindent(xstr,gv_indent,outfile)
            continue

        if str(thing) in mod_list:
            xstr = f"'{str(thing)}':"
        else:
            xstr = f"{thing}:"
        prindent(xstr,gv_indent,outfile)
        if atyp == dict:
            gv_indent += tsize
            yaml_write(adict[thing], tsize, outfile)
            gv_indent -= tsize
        elif atyp == list:
            gv_indent += tsize
            the_list = adict[thing]
            for item in the_list:
                btyp = type(item)
                if btyp != dict and btyp != list:
                    xstr = f"- {item}"
                    prindent(xstr,gv_indent,outfile)
                    continue
                xstr = f"- "
                prindent(xstr,gv_indent,outfile)
                gv_indent += tsize
                yaml_write(item, tsize, outfile)
                gv_indent -= tsize
            gv_indent -= tsize


def contract_dsspec_branches(dataset_spec):

    # Phase 1:  walk the E3SM cases to generate case_branches[case_id] = branch, case_id_list[n] = case_id
    case_branches = dict()
    case_id_list = list()

    for model_version in dataset_spec['project']['E3SM']:
        for experiment, experimentinfo in dataset_spec['project']['E3SM'][model_version].items():
            case_id = f"E3SM.{model_version}.{experiment}"
            case_branches[case_id] = experimentinfo['resolution']
            case_id_list.append(case_id)

    # phase 2: create unique "case_extensions[extn_id] = dict()" and "extension_members[extn_id] = list(case_ids)"
    extn_num = 0
    case_extensions = dict()
    extension_members = dict()
    while len(case_id_list) > 0:
        extn_num += 1
        extn_id = f"CASE_EXTENSION_{extn_num:02d}"
        case_id = case_id_list.pop(0)
        case_extensions[extn_id] = case_branches[case_id]       # first is unique
        extension_members[extn_id] = list()
        extension_members[extn_id].append(case_id)
        case_id_residue = list()
        while len(case_id_list) > 0:
            a_case_id = case_id_list.pop(0)
            if case_branches[a_case_id] == case_extensions[extn_id]:
                extension_members[extn_id].append(a_case_id)
            else:
                case_id_residue.append(a_case_id)
        case_id_list = case_id_residue
        
    # phase 3: for each extn_id in extension_members, use each member to find the E3SM case and set its "resolution" to the extn_id
    # then, add the "case_extensions" to the dataset_spec['CASE_EXTENSIONS']
    for extn_id in extension_members:
        for a_member in extension_members[extn_id]:
            _, model_version, experiment = a_member.split('.')
            dataset_spec['project']['E3SM'][model_version][experiment]['resolution'] = extn_id
    dataset_spec['CASE_EXTENSIONS'] = case_extensions

    # DEBUG
    # yaml_write(case_extensions,4,"Case_Extensions")
    # yaml_write(extension_members,4,"Extension_Members")

        
def load_yaml(yaml_path):
    with open(yaml_path, 'r') as instream:
        in_yaml = yaml.load(instream, Loader=yaml.SafeLoader)

    return in_yaml


def main():

    pargs = assess_args()

    ds_spec = load_yaml(pargs.in_dsspec)

    contract_dsspec_branches(ds_spec)

    yaml_write(ds_spec,4,pargs.out_dsspec)

    sys.exit(0)

if __name__ == "__main__":
    sys.exit(main())


