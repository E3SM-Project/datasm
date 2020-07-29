import os, sys, argparse

# Given two files of lines, convert each to sets of lines A and B.
# Output the set differences A-B and B-A, and intersection A&B.

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
    set1 = set([ aline for aline in contents if aline[:-1] ])
    with open(args.file2) as f:
        contents = f.read().split('\n')
    set2 = set([ aline for aline in contents if aline[:-1] ])

    F1,F2,FB = trisect(set1,set2)

    F1=list(F1)
    F2=list(F2)
    FB=list(FB)
    F1.sort()
    F2.sort()
    FB.sort()

    print(f'ONLY {args.file1}')
    for item in F1:
	    print(f'    { item }')

    print(f'ONLY {args.file2}')
    for item in F2:
	    print(f'    { item }')

    print(f'BOTH {args.file1} and {args.file2}')
    for item in FB:
	    print(f'    { item }')

    sys.exit(0)

if __name__ == "__main__":
  sys.exit(main())


