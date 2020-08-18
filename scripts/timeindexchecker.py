import os
import sys
import re
import argparse
import xarray as xr
from tqdm import tqdm
from pprint import pprint
import concurrent.futures


def check_indices(f1, f2):

    ds1 = xr.open_dataset(f1, decode_times=False)
    ds2 = xr.open_dataset(f2, decode_times=False)

    # the ending bound of the first file       
    last_first_bound = ds1['time_bnds'][-1].values[-1]
    # the first bound of the second file
    first_last_bound = ds2['time_bnds'][0].values[0]

    if last_first_bound != first_last_bound:
        _, n1 = os.path.split(f1)
        _, n2 = os.path.split(f2)
        msg = f"{n1}, {n2}"
        return msg
    return False

def dispatcher(inpath, num_jobs):
    names = [os.path.join(inpath, x) for x in os.listdir(inpath) if x.endswith('.nc')]

    pattern = r'\d{4}-\d{2}-\d{2}'
    files = []
    for name in names:
        start = re.search(pattern, name).start()
        files.append({
            'prefix': name[:start],
            'suffix': name[start:]
        })
    
    files = sorted(files, key=lambda i: i['suffix'])
    files = [x['prefix'] + x['suffix'] for x in files]

    pairs = zip(files[:-1], files[1:])
    results = []

    print(f'Starting overlap check with {num_jobs} workers')
    futures = []
    overlap = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_jobs) as pool:
        futures = [pool.submit(check_indices, pair[0], pair[1]) for pair in pairs]
    
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            overlap.append(future.result())
    
    # since the results dont come back in sorted order, sort them again    
    overlap = [x for x in overlap if x]
    segments = []
    for item in overlap:
        start = re.search(pattern, item.split(', ')[0]).start()
        segments.append({
            'prefix': item[:start],
            'suffix': item[start:],
            'contents': item
        })
    segments = [x['contents'].split(', ') for x in sorted(segments, key=lambda i: i['suffix'])]


    # remove all the False items
    # segments = [x.split(', ') for x in overlap]
    for s in segments:
        print(s)

    segpairs = [x for x in zip(segments[:-1], segments[1:])]
    
    print(f"first segment starts at {segments[0]}")

    is_continuing = True
    for idx, segpair in enumerate(segpairs):
        if idx == 0:
            continue
        if is_continuing:
            if segpair[0][1] != segpair[1][0]:
                print(f"segment ends at {segpair[0][1]}")
                is_continuing = False
        else:

            print(f"segment starts at {segpair[0][0]} - {segpair[0][1]}")
            is_continuing = True

    print(f"segment ends at {segpairs[-1][-1][-1]}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', help="The directory to check for time index issues, should only contain a single time-frequency from a single case")
    parser.add_argument('--jobs', help="the number of processes, default is 8", default=8, type=int)
    args = parser.parse_args()
    return dispatcher(args.input, args.jobs)

if __name__ == "__main__":
    sys.exit(main())