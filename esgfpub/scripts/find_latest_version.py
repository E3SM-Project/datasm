import sys
import os
import argparse
from subprocess import Popen, PIPE

DESC = "Find all the latest version directories under a CMIP directory tree"

def main():
    parser = argparse.ArgumentParser(description=DESC)
    parser.add_argument('root')
    args = parser.parse_args()

    cmd = 'find {root} -type d -name gr'.format(root=args.root)
    gr_paths, _ = Popen(cmd.split(), stdout=PIPE).communicate()

    gr_paths = gr_paths.decode('utf-8').split()

    inpaths = []

    for path in gr_paths:
        # get the variable
        variable = path.split(os.sep)[-2]

        # remove the "v"
        versions = sorted([x[1:] for x in os.listdir(path)])
        inpaths.append((variable, os.path.join(path, "v{}".format(versions[-1]))))
    
    for var, path in inpaths:
        print(path)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())