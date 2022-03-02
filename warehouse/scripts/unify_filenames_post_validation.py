import sys, os
import argparse
from argparse import RawTextHelpFormatter
import glob
import shutil
import subprocess
import time
import pytz
import re

# 
def ts():
    return 'TS_' + pytz.utc.localize(datetime.utcnow()).strftime("%Y%m%d_%H%M%S_%f")


helptext = '''
    Given a directory of ".nc" files containing a sim-date part of the form "nnnn-nn", this utility will
    record the "static_lead" part of the first file <static_lead>nnnn-nn*anything*.nc, and then
    1. Ensure that ANY file <begin_part>nnnn-nn is converted to <static_lead>nnnn-nn,
    2. Ensure that ANY file containing <part1>(n)<part2> is converted to <part1><part2>,
    3. Ensure that ANY file containing <part1>.trunc<part2> is converted to <part1><part2>

    Unless "--force" is supplied, any conversion that would result in a file-collision will be avoided
    and a WARNING given.

    if "--dryrun" is supplied, no changes are made, but the changes that would have occurred are announced.

'''

def assess_args():

    parser = argparse.ArgumentParser(description=helptext, prefix_chars='-', formatter_class=RawTextHelpFormatter)
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')
    required.add_argument('-i', '--input', action='store', dest="inputdir", type=str, required=True)
    optional.add_argument('--dryrun', action='store_true', dest="dryrun", required=False)
    optional.add_argument('--force', action='store_true', dest="force", required=False)

    args = parser.parse_args()

    if not (args.inputdir):
        print("Error:  Must supply -i input directory for name unification. Try -h")
        sys.exit(0)

    return args

def get_file_list(path):
    for root, dirs, files in os.walk(path):
        if files:
            return files
        else:
            return list()   

def force_consistent_static_name(filelist, args):
    date_pat = r"\d{4}-\d{2}"
    fcount = 0
    mcount = 0
    static_part = ""
    for fname in filelist:
        date_pos = re.search(date_pat,fname)
        if date_pos:
            fcount += 1
            atup = date_pos.span()
            lead_part = fname[0:atup[0]]
            if fcount == 1:
                static_part = lead_part
                continue
            if lead_part == static_part:
                continue
            tail_part = fname[atup[0]:]
            newname = f"{static_part}{tail_part}"
            if newname in filelist:
                if not args.force:
                    print(f"WARNING: cannot convert {fname} to {newname} without name collision")
                    continue
            print(f"CONVERT: {fname} to {newname}")
            mcount += 1
            if args.dryrun:
                continue
            srcpath = os.path.join(args.inputdir,fname)
            dstpath = os.path.join(args.inputdir,newname)
            os.rename(srcpath,dstpath)
        else:
            # print(f"found nothing in fname {fname}")
            continue

    return mcount

def suppress_parenthetic_replacements(filelist, args):
    par_part = r"\(\d{1}\)"
    fcount = 0
    mcount = 0
    for fname in filelist:
        fcount += 1
        par_pos = re.search(par_part,fname)
        if par_pos:
            atup = par_pos.span()
            pre_str = fname[0:atup[0]]
            aft_str = fname[atup[1]:]
            newname = f"{pre_str}{aft_str}"
            if newname in filelist:
                if not args.force:
                    print(f"WARNING: cannot convert {fname} without name collision")
                    continue
            print(f"CONVERT: {fname} to {newname}")
            mcount += 1
            if args.dryrun:
                continue
            srcpath = os.path.join(args.inputdir,fname)
            dstpath = os.path.join(args.inputdir,newname)
            os.rename(srcpath,dstpath)
                
    return mcount

def suppress_dot_trunc_names(filelist, args):
    par_part = r"\.trunc"
    fcount = 0
    mcount = 0
    for fname in filelist:
        fcount += 1
        par_pos = re.search(par_part,fname)
        if par_pos:
            atup = par_pos.span()
            pre_str = fname[0:atup[0]]
            aft_str = fname[atup[1]:]
            newname = f"{pre_str}{aft_str}"
            if newname in filelist:
                if not args.force:
                    print(f"WARNING: cannot convert {fname} without name collision")
                    continue
            print(f"CONVERT: {fname} to {newname}")
            mcount += 1
            if args.dryrun:
                continue
            srcpath = os.path.join(args.inputdir,fname)
            dstpath = os.path.join(args.inputdir,newname)
            os.rename(srcpath,dstpath)
                
    return mcount

def unify_filenames(apath):
    filelist = get_file_list(apath)
    filelist.sort()
    if not filelist or not len(filelist):
        con_message("error", f"move_to_publication: unify_filenames: directory {apath} does not exist or has no files")
        return(1)

    mods1 = force_consistent_static_name(filelist, args)

    print(f"Pass 1: converted {mods1} files to consistent static part")
    # con_message("info", f"move_to_publication:unify_filenames: converted {mods1} files to consistent static part")

    filelist = get_file_list(apath)
    filelist.sort()
    mods2 = suppress_parenthetic_replacements(filelist, args)

    print(f"Pass 2: suppressed {mods2} parenthetic replacements")
    # con_message("info", f"move_to_publication:unify_filenames: suppressed {mods2} parenthetic replacements")

    filelist = get_file_list(apath)
    filelist.sort()
    mods3 = suppress_dot_trunc_names(filelist, args)

    print(f"Pass 3: suppressed {mods3} dot_trunc names")
    # con_message("info", f"move_to_publication:unify_filenames: suppressed {mods3} suppressed {mods2} dot_trunc names")

    return 0



def main():

    args = assess_args()

    unify_filenames(args.inputdir)

    sys.exit(0)


if __name__ == "__main__":
  sys.exit(main())







