import os, sys, argparse

# Given two files of lines, convert each to sets of lines A and B.
# Output the set differences A-B and B-A, and intersection A&B.

def print_file_list(outfile, items):
    with open(outfile, "w") as outstream:
        for x in items:
            outstream.write(f"{x}\n")

def trisect(A, B):
	return A-B, B-A, A&B

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('file1', metavar='F1', type=str,
                        help='first file for the process')
    parser.add_argument('file2', metavar='F2', type=str,
                        help='second file for the process')
    args = parser.parse_args()

    with open(args.file1) as f:
        contents = f.read().split('\n')
    # set1 = set([ aline for aline in contents if aline[:-1] ])
    set1 = set([ aline for aline in contents if len(aline) ])
    with open(args.file2) as f:
        contents = f.read().split('\n')
    # set2 = set([ aline for aline in contents if aline[:-1] ])
    set2 = set([ aline for aline in contents if len(aline) ])

    F1,F2,FB = trisect(set1,set2)

    F1=list(F1)
    F2=list(F2)
    FB=list(FB)
    F1.sort()
    F2.sort()
    FB.sort()

    print(f'ONLY {args.file1} : {len(F1)} items')
    print_file_list(f"only-{args.file1}",F1)

    print(f'ONLY {args.file2} : {len(F2)} items')
    print_file_list(f"only-{args.file2}",F2)

    print(f'BOTH {args.file1} and {args.file2} : {len(FB)} items')
    print_file_list(f"both-{args.file1}-and-{args.file2}",FB)

    sys.exit(0)

if __name__ == "__main__":
  sys.exit(main())


