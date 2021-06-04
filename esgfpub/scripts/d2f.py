import os
import sys
import argparse
from tqdm import tqdm
from subprocess import Popen, PIPE
from concurrent.futures import ProcessPoolExecutor, as_completed
from tempfile import TemporaryDirectory

def d2f(inpath, outpath):
    cmd = f'ncpdq -M dbl_flt {inpath} {outpath}'.split()
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    out = out.decode('utf-8')
    err = err.decode('utf-8')
    if err:
        raise ValueError(err)
    return outpath

def zMid(inpath, restart, outpath):
    cmd = f"ocean_add_zmid -i {inpath} -c {restart} -o {outpath}".split()
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    out = out.decode('utf-8')
    err = err.decode('utf-8')
    if err:
        print(err)
    print(out)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input', type=str, help="path to raw ocean files directory")
    parser.add_argument('restart', type=str, help="path to a single mpaso restart file")
    parser.add_argument('output', type=str, help="path to processed output directory")
    parser.add_argument('-n', '--num-workers', type=int, default=8, help="number of parallel workers")
    parser.add_argument('-q', '--quite', action="store_true", help="don't output progressbars or status messages")
    args = parser.parse_args()
    os.makedirs(args.output, exist_ok=True)

    files = os.listdir(args.input)
    with TemporaryDirectory() as tempdir:
        with ProcessPoolExecutor(max_workers=args.num_workers) as pool:
            
            d2f_futures = []
            for file in files:
                inpath = os.path.join(args.input, file)
                temppath = os.path.join(tempdir, file)
                d2f_futures.append(pool.submit(
                    d2f, inpath, temppath))

            zmid_futures = []
            for future in tqdm(as_completed(d2f_futures), total=len(files), desc="Running d2f conversion", disable=args.quite):
                file = future.result()
                _, name = os.path.split(file)
                outpath = os.path.join(args.output, name)
                
                zmid_futures.append(pool.submit(
                    zMid, file, outpath))
            
            for _ in tqdm(as_completed(zmid_futures), total=len(files), desc="Running zMid", disable=args.quite):
                pass
    return 0


if __name__ == "__main__":
    sys.exit(main())