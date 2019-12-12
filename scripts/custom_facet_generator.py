import sys
import os
import argparse

__version__ = "0.1.0"


def parse_args():
    help_text = """
Produce pipe-separated "DatasetID | facet1 | facet2 | ... " lines for each map-file entry.
Provide --map-dir mapfiledirectory --facets key1=value1 key2=value2 . . .
Quote-protect values having embedded whitespace."""

    parser = argparse.ArgumentParser(
        description=help_text)
    parser.add_argument(
        "-v", "--version",
        action="store_true",
        help="show program version")
    parser.add_argument(
        "-m", '--map-dir',
        required=True,
        dest='mapdir',
        help="directory of map files")
    parser.add_argument(
        '--facets',
        nargs='+',
        required=True,
        help="sequence of var=value pairs")
    parser.add_argument(
        '-o', '--output',
        default="custom_facets.map"
        help="output for the new mapfile, defaults to $PWD/custom_facets.map")
    parser.add_argument(
        '--debug',
        action="store_true",
        help="turn on debug prints")

    args = parser.parse_args()

    if args.version:
        print("custom_facet_generator version {}".format(__version__))
        sys.exit(0)
    return args


def main():

    args = parse_args()

    facet_str = ""

    for facet in args.facets:

    facet_str = " | ".join(args.facets)
    if args.debug:
        print("facet string")
        print(facet_str)

    maplist = [os.path.join(args.mapdir, f) for f in os.listdir(
        args.mapdir) if os.path.isfile(os.path.join(args.mapdir, f))]
    if args.debug:
        for item in maplist:
            print("mapfiles")
            print(item)

    output = []

    for m in maplist:
        with open(amap, "r") as amaplines:
            aline = amaplines.readline()
            datasetID = aline.split(' ')[0]
            output.append(datasetID + facet_str + '\n')

    with open(args.output, 'w') as outfile:
        for line in output:
            outfile.write(line)

    return 0


if __name__ == "__main__":
    sys.exit(main())
