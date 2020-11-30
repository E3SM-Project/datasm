import sys
import os
import argparse
from subprocess import Popen, PIPE

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('root')
    parser.add_argument('output')
    args = parser.parse_args()

    if not os.path.exists(args.output):
        print(f"creating output directory {args.output}")
        os.makedirs(args.output)

    cmd = f'find {args.root} -type d -name gr'
    gr_paths, _ = Popen(cmd.split(), stdout=PIPE).communicate()

    gr_paths = gr_paths.decode('utf-8').split()

    inpaths = []

    for path in gr_paths:
        # get the variable
        variable = path.split(os.sep)[-2]

        # remove the "v"
        versions = [x[1:] for x in os.listdir(path)]
        inpaths.append((variable, os.path.join(path, f"v{versions[-1]}")))
    
    for var, path in inpaths:
        dst = os.path.join(args.output, var)
        os.symlink(path, dst)

    return 0

if __name__ == "__main__":
    sys.exit(main())