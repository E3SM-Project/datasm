import sys, os
import argparse
from argparse import RawTextHelpFormatter
from pathlib import Path
import json
import shutil
import time
from datetime import datetime, timezone

# 
def ts():
    return 'TS_' + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")


helptext = '''
    Supply -i/--infile full path to json metadata file.  Supply --mode [get|set].
    If mode == get, if metadata has "version", it will be returned, else "NONE".
    If mode == set, the metadata version will be set to the current UTC date (vYYYYMMDD)".
'''

def assess_args():

    parser = argparse.ArgumentParser(description=helptext, prefix_chars='-', formatter_class=RawTextHelpFormatter)
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')
    required.add_argument('-i', '--infile', action='store', dest="inmeta", type=str, required=True)
    required.add_argument('-m', '--mode', action='store', dest="mode", type=str, required=True)

    args = parser.parse_args()

    return args

def json_readfile(filename):
    with open(filename, "r") as file_content:
        json_in = json.load(file_content)
    return json_in

def set_version_in_user_metadata(metadata_path, dsversion):     # set version "vYYYYMMDD" in user metadata

    in_data = json_readfile(metadata_path)
    in_data["version"] = dsversion
    json_writefile(in_data,metadata_path)

def json_writefile(indata, filename):
    exdata = json.dumps( indata, indent=2, separators=(',\n', ': ') )
    with open(filename, "w") as file_out:
        file_out.write( exdata )




def main():

    args = assess_args()

    meta_file = Path(args.inmeta)
    if not meta_file.exists():
        print(f"No such file: {args.inmeta}")
        sys.exit(0)
        
    mode = args.mode
    if mode == "get":
        # ds = xr.open_dataset(meta_file)
        in_json = json_readfile(meta_file)
        if 'version' in in_json.keys():
            meta_version = in_json['version']
        else:
            meta_version = 'NONE'

        print(f"{meta_version}")

    if mode == "set":
        version = 'v' + datetime.now(timezone.utc).strftime("%Y%m%d")
        set_version_in_user_metadata(meta_file, version)

    sys.exit(0)

if __name__ == "__main__":
  sys.exit(main())





