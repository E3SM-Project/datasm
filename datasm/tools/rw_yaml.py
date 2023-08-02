import os
import sys
import argparse
import yaml
from argparse import RawTextHelpFormatter


helptext = '''
    Usage:  python rw_yaml.py  -i inputyaml -o outputyaml [-s|--sort] [-t tabsize| --tabsize tabsize]

    Rewrite a yaml file, optionally sorting dictionary keys and changing the tabsize.
    Blank lines are eliminated.  Not tested with comments.
'''

def assess_args():

    parser = argparse.ArgumentParser(description=helptext, prefix_chars='-', formatter_class=RawTextHelpFormatter)
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    required.add_argument('-i', '--infile', action='store', dest="infile", type=str, required=True)
    required.add_argument('-o', '--outfile', action='store', dest="outfile", type=str, required=True)
    optional.add_argument('-s', '--sort', action='store_true', dest="sort", default=False, required=False)
    optional.add_argument('-t', '--tabsize', action='store', dest="tabsize", type=int, default=4, required=False)


    args = parser.parse_args()

    return args

def load_yaml(inpath):
    with open(inpath, 'r') as instream:
        in_yaml = yaml.load(instream, Loader=yaml.SafeLoader)
    return in_yaml

def dict_reorder(item):
    return {k: dict_reorder(v) if isinstance(v, dict) else v for k, v in sorted(item.items())}

gv_indent = 0
gv_dosort = False
gv_outfile = ""

def prindent(astr,indent):
    global gv_outfile
    spaces="                                                                                "
    outstr=f"{spaces[:indent]}{astr}"
    with open(gv_outfile, 'a+') as f:
        f.write(f'{outstr}\n')
        # print(f"{outstr}")

# recurse on dict or list ...

def yaml_write(adict, tsize):
    global gv_indent
    global gv_dosort

    if gv_dosort:
        adict = dict_reorder(adict)

    for thing in adict:
        atyp = type(adict[thing])
        if atyp != dict and atyp != list:
            xstr = f"{thing}: {adict[thing]}"
            prindent(xstr,gv_indent)
            continue

        xstr = f"{thing}:"
        prindent(xstr,gv_indent)
        if atyp == dict:
            gv_indent += tsize
            yaml_write(adict[thing], tsize)
            gv_indent -= tsize
        elif atyp == list:
            gv_indent += tsize
            the_list = adict[thing]
            for item in the_list:
                btyp = type(item)
                if btyp != dict and btyp != list:
                    xstr = f"- {item}"
                    prindent(xstr,gv_indent)
                    continue
                xstr = f"- "
                prindent(xstr,gv_indent)
                gv_indent += tsize
                yaml_write(item, tsize)
                gv_indent -= tsize
            gv_indent -= tsize


def main():
    global gv_dosort
    global gv_outfile

    pargs = assess_args()

    '''
    print(f"pargs.infile = {pargs.infile}")
    print(f"pargs.outfile = {pargs.outfile}")
    print(f"pargs.sort = {pargs.sort}")
    print(f"pargs.tabsize = {pargs.tabsize}")
    '''

    gv_outfile = pargs.outfile
    gv_dosort = pargs.sort

    in_dict = load_yaml(pargs.infile)
    yaml_write(in_dict,pargs.tabsize)

    sys.exit(0)


if __name__ == "__main__":
    sys.exit(main())


