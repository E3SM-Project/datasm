import sys
import os
import argparse
from argparse import RawTextHelpFormatter
from pathlib import Path
import glob
import json
import shutil
import subprocess
import time
import pytz
from datetime import datetime

# 
def ts():
    return 'TS_' + pytz.utc.localize(datetime.utcnow()).strftime("%Y%m%d_%H%M%S_%f")


helptext = '''
    Supply either:

        -i <path to json file> -m get -k <key_name>
    or
        -i <path to json file> -m set -k <key_name> -v <key_value>

'''

def assess_args():

    parser = argparse.ArgumentParser(description=helptext, prefix_chars='-', formatter_class=RawTextHelpFormatter)
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')
    required.add_argument('-i', '--infile', action='store', dest="injson", type=str, required=True)
    required.add_argument('-m', '--mode', action='store', dest="mode", type=str, required=True)
    required.add_argument('-k', '--key', action='store', dest="key", type=str, required=True)
    optional.add_argument('-v', '--val', action='store', dest="val", type=str, required=False)

    args = parser.parse_args()

    if args.injson[-5:] != ".json":
        print(f"Not a json file: {args.injson} ({args.injson[-5:]})")
        sys.exit(0)

    if not os.path.exists(args.injson):
        print(f"No such file: {args.injson}")
        sys.exit(0)

    if args.mode == "set" and not args.val:
        print(f"No value specified for setting {args.key}")
        sys.exit(0)
        
    return args

def json_readfile(filename):
    with open(filename, "r") as file_content:
        json_in = json.load(file_content)
    return json_in

def get_value_in_json(json_data, keyname): 

    if keyname in json_data.keys():
        return json_data[keyname]
    return 'NONE'

def set_value_in_json(json_data, keyname, keyvalue): 

    json_data[keyname] = keyvalue

def json_writefile(exdata, filename):
    exdata_str = json.dumps( exdata, indent=2, separators=(',\n', ': ') ) + '\n'
    with open(filename, "w") as file_out:
        file_out.write( exdata_str )




def main():

    args = assess_args()

    in_data = json_readfile(args.injson)

    mode = args.mode
    if mode == "get":
        aval = get_value_in_json(in_data, args.key)
        print(f"{aval}")

    if mode == "set":
        set_value_in_json(in_data, args.key, args.val)
        json_writefile(in_data,args.injson)

    sys.exit(0)

if __name__ == "__main__":
  sys.exit(main())





