import os, sys, argparse
import yaml
from argparse import RawTextHelpFormatter


helptext = '''
    Usage:  python expand_dsspec_branches.py  -i dataset_spec_contracted.yaml -o dataset_spec_expanded.yaml

    Reads in the E3SM dataset_spec (example: /p/user_pub/e3sm/staging/resource/dataset_spec.yaml).
    (Assumes the dataset_spec is in contracted form, and has a "CASE_EXTENSIONS:" section.)
    For each Project=E3SM Model_Version and Experiment, it locates the branch labeled "resolution"
    and uses the "Extension_ID" found there to locate that branch in the CASE_EXTENSIONS table,
    replacing the Extension_ID with the actual branch content.  The CASE_EXTENSIONS table is then
    removed, and the resulting "expanded" dataset_spec is output to the indicated output file.
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
        

def load_yaml_spec(yaml_spec_path):
    with open(yaml_spec_path, 'r') as instream:
        yaml_spec = yaml.load(instream, Loader=yaml.SafeLoader)

    return yaml_spec


def main():

    pargs = assess_args()

    ds_spec = load_yaml_spec(pargs.in_dsspec)

    if 'CASE_EXTENSIONS' in ds_spec.keys():
        expand_dataset_spec(ds_spec)
        del ds_spec['CASE_EXTENSIONS']
        yaml_write(ds_spec,4,pargs.out_dsspec)   
    else:
        print(f"ERROR: Cannot expand dataset - no CASE_EXTENSIONS table found: {pargs.in_dsspec}")


    sys.exit(0)

if __name__ == "__main__":
    sys.exit(main())


