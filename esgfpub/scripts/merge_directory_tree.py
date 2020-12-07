import sys
import os
import shutil
import argparse
from functools import reduce
from pathlib import Path

move_methods = {
    'move': shutil.move,
    'copy': shutil.copy,
    'link': os.symlink
}

def main():
    DESC = "Recursively merge the SOURCE directory tree into the DESTINATION tree"
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help="The source tree")
    parser.add_argument('destination', help="The destination tree")
    parser.add_argument('--mode', help="What method to move the files, allowed values are: (default) copy, move, or link", default='move')
    parser.add_argument('--over-write', action="store_true", help="If the file already exists on the destination, over-write it. Default is False")
    parser.add_argument('--dryrun', action="store_true", help="Only print out what would be moved, but dont move anything")
    args = parser.parse_args()

    if args.mode and args.mode not in move_methods.keys():
        raise ValueError(f"{args.mode} is not an allowed value, use one of {move_methods.keys()}")
    move = move_methods.get(args.mode, move_methods['copy'])

    source = Path(args.source)
    dest = Path(args.destination)
    
    for root, dirs, files in os.walk(source):
        if Path(root) == source:
            for name in files:
                ofile = Path(source, name)
                nfile = Path(dest, name)
                if nfile.exists():
                    if not args.over_write:
                        print(f"{ofile} exists at destination, skipping")
                        continue
                if not args.dryrun:
                    move(ofile.resolve(), nfile.resolve())
            continue
        if not files:
            continue
        for name in files:
            ofile = Path(root, name)
            nfile = Path(dest, str(ofile)[len(str(args.source)):])
            
            if not nfile.parent.exists() and not args.dryrun:
                print(f"Making directory {nfile.parent}")
                os.makedirs(nfile.parent)

            if nfile.exists():
                if not args.over_write:
                    print(f"{ofile} exists at destination, skipping")
                    continue

            print(f"{args.mode} {ofile} -> {nfile}")
            if not args.dryrun:
                move(ofile.resolve(), nfile.resolve())
    return 0

if __name__ == "__main__":
    sys.exit(main())