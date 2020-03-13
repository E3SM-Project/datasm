import warnings
warnings.simplefilter('ignore')

import os
from datetime import datetime
from tqdm.auto import tqdm

import xarray as xr
import vcs
from dask.diagnostics import ProgressBar
from dask.distributed import get_client, as_completed
from esgfpub.util import path_to_dataset_id, print_message


def run_minmaxmean(ds, varname, dataset_id=None, debug=False):

    if debug:
        print_message("Computing min-max-mean for {}".format(dataset_id), 'info')

    ds['mins'] = ds[varname].min(dim=('lat', 'lon'))
    ds['maxs'] = ds[varname].max(dim=('lat', 'lon'))
    ds['means'] = ds[varname].mean(dim=('lat', 'lon'))
    ds.compute()
    return ds


def plot_minmaxmean(outpath, ds, dataset_id, debug=False):
    
    if debug:
        print_message("plotting {}".format(dataset_id), 'info')
    times = {}
    for i, t in enumerate(ds['time']):
        if i % (12 * 10) == 0: # a tick every 10 years
            v = str(t.values)
            times[i] = v[:4]
    
    mx = ds['maxs'].max().compute().values.item()
    mn = ds['mins'].min().compute().values.item()

    canvas = vcs.init(geometry=(800, 600))
    template = vcs.createtemplate()


    canvas.clear()
    line = vcs.create1d()
    line.linecolor = "red"
    line.linewidth = 1.  
    line.marker = None
    line.markersize = 1.2
    line.xticlabels1 = times
    line.datawc_y2 = mx
    line.datawc_y1 = mn

    canvas.plot(ds['maxs'], template, line, id=[d for d in ds.data_vars if 'bnds' not in d].pop(), title=dataset_id)

    template.blank(["mean", "crdate", "crtime", "min", "max", "dataname"])
    line.linecolor = "black"
    canvas.plot(ds['mins'], template, line)

    line.linecolor = "green"
    canvas.plot(ds['means'], template, line)
    canvas.png(outpath)
    if debug:
        print_message("plot available at {}".format(outpath), 'info')
    canvas.close()
    return

def check_step(ds, m, mean, stddev, dataset_id):
    msg = None
    sq_variance = pow((m - mean)/stddev, 2)
    if sq_variance > 20:
        msg = "\tIssue found for {} at {}".format(dataset_id, str(m['time'].item())[:7])
    return msg

def segment_sq_variance(ds, dataset_id):
    stddev = ds['means'].std().compute().item()
    mean = ds['means'].mean().compute().item()
    
    futures = []
    client = get_client()
    for m in ds['means']:
        futures.append(
            client.submit(
                check_step, ds, m, mean, stddev, dataset_id))
    issues = [x for x in client.gather(futures) if x]
    ds.close()
    return issues

def check_sq_variance(ds, dataset_id, debug=False):

    if debug:
        print_message("Starting sq-variance check for {}".format(dataset_id), 'info')
        
    futures = []
    num_segments = 4
    client = get_client()
    segments = list(range(0, ds['time'].size, ds['time'].size//num_segments))
    for idx, seg in enumerate(segments):
        if idx == num_segments - 1:
            seg_end = ds['time'].size
        else:
            seg_end = segments[idx + 1]
        temp_ds = ds['time'][seg: seg_end]
        
        futures.append(
            client.submit(
                segment_sq_variance, ds.sel(time=temp_ds), dataset_id))

    issues = client.gather(futures)
    return issues


def verify_dataset(dataset_path, dataset_id, variable, output, only_plots=False, debug=False):

    messages, issues = list(), list()
    pngpath = os.path.join(output, "{}.png".format(dataset_id))
    
    ds = xr.open_mfdataset(dataset_path + '/*.nc', autoclose=True)
    if 'lat' not in ds.coords or 'lon' not in ds.coords or 'time' not in ds.coords:
        return [], dataset_id

    ds = run_minmaxmean(ds, variable, dataset_id, debug=debug)
    plot_minmaxmean(pngpath, ds, dataset_id, debug=debug)
    if debug:
        start = datetime.now()

    if not only_plots:
        issues = check_sq_variance(ds, dataset_id, debug=debug)
    ds.close()
        
    if debug:
        end = datetime.now()
        print_message("finished variance check of {} after {}".format(dataset_id, end-start), 'info')
    return issues, dataset_id

