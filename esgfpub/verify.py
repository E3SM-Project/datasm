import warnings
warnings.simplefilter('ignore')

from esgfpub.util import path_to_dataset_id, print_message
from dask.distributed import get_client, worker_client, as_completed
from dask.diagnostics import ProgressBar
import vcs
import xarray as xr
import numpy as np
from datetime import datetime
import os
from tqdm import tqdm

import logging
logger = logging.getLogger("distributed.worker")
logger.setLevel(logging.ERROR)


def plot_minmaxmean(outpath, ds, vmax, dataset_id, debug=False):
    # import ipdb; ipdb.set_trace()
    # if debug:
    #     print_message("plotting {}".format(dataset_id), 'info')
    times = {}
    for i, t in enumerate(ds['time']):
        if i % (12 * 10) == 0:  # a tick every 10 years
            v = str(t.values)
            times[i] = v[:4]

    mx = ds['maxs'].max().compute().values.item()
    mn = ds['maxs'].min().compute().values.item()
    # import ipdb; ipdb.set_trace()
    vmax = [x for x in vmax if not np.isnan(x)]

    diff = (mx - mn)/20

    canvas = vcs.init(geometry=(800, 600))
    template = vcs.createtemplate()

    canvas.clear()
    line = vcs.create1d()
    line.linecolor = "black"
    line.linewidth = 1.
    line.marker = None
    line.xticlabels1 = times
    line.datawc_y2 = mx + diff
    line.datawc_y1 = mn - diff

    canvas.plot(ds['maxs'], template, line, id="", title=dataset_id)
    # canvas.plot(vmax, line, title=dataset_id)
    
    template.blank(["mean", "crdate", "crtime", "min", "max", "dataname"])
    mx = max(vmax)
    mn = min(vmax)
    line.linecolor = "red"
    diff = (mx - mn)/20
    line.datawc_y2 = mx + diff
    line.datawc_y1 = mn - diff
    line.xticlabels1 = {}
    line.yticlabels1 = {}
    canvas.plot(vmax, template, line)

    canvas.png(outpath)
    canvas.close()
    return


def run_chunk(ds, dataset_id, maxrollingvar, maxrollingstd, segment, index):
    issues = list()
    vmax = list()
    # vmin = list()
    # import ipdb; ipdb.set_trace()
    for step in ds['time']:
        
        # if ds.sel(time=step)['maxs'].shape != (1):
        #     mx = ds.sel(time=step)['maxs'][0].values
        mx = ds.sel(time=step)['maxs'].values
        mn = ds.sel(time=step)['mins'].values
        if not isinstance(mx, int) and not isinstance(mx, float) and mx.size > 1:
            mx = max(mx)
        if not isinstance(mn, int) and not isinstance(mn, float) and mn.size > 1:
            mn = min(mn)
        if mx == 0.0 and mn == 0.0:
            issues.append("\tZero data issue found for {} at {}".format(
                    dataset_id, str(step['time'].item())[:7]))
            vmax.append(0)
            # vmin.append(0)
            continue

        max_rolling_var = maxrollingvar.sel(time=step)
        mx_rolling_std = maxrollingstd.sel(time=step)
        # min_rolling_step = minrolling.sel(time=step)
        if np.isnan(max_rolling_var) or np.isnan(mx_rolling_std) or np.isnan(mx):
            vmax.append(0)
            # vmin.append(0)
            continue

        # mn_rolling_std = minrollingstd.sel(time=step)

        max_variance = abs((mx - max_rolling_var)/mx_rolling_std).values.item()
        # min_variance = abs((mn - min_rolling_step)/mn_rolling_std).values.item()

        if max_variance > 0.9:
            issues.append("\tmax variance issue found for {} at {}".format(
                dataset_id, str(step['time'].item())[:7]))
        vmax.append(max_variance)
        # vmin.append(min_variance)
    return vmax, issues, segment

def check_sq_variance(dataset_path, dataset_id, variable, pngpath, only_plots=False, debug=False):
    issues = []
    client = get_client()
    with xr.open_mfdataset(dataset_path + '/*.nc') as ds:#, worker_client() as client:
        
        if 'time' not in ds.coords:
            return [], dataset_id
        
        # import ipdb; ipdb.set_trace()
        if 'depth' in ds.coords and ds['depth'].size > 1:
            ds['mins'] = ds[variable].min(dim=('lat', 'lon', 'depth'))
            ds['maxs'] = ds[variable].mean(dim=('lat', 'lon', 'depth'))
            # ds['means'] = ds[varname].mean(dim=('lat', 'lon', 'depth'))
        elif 'lev' in ds.coords:
            ds['mins'] = ds[variable].min(dim=('lat', 'lon', 'lev'))
            ds['maxs'] = ds[variable].mean(dim=('lat', 'lon', 'lev'))
            # ds['means'] = ds[varname].mean(dim=('lat', 'lon', 'lev'))
        elif 'plev' in ds.coords:
            ds['mins'] = ds[variable].min(dim=('lat', 'lon', 'plev'))
            ds['maxs'] = ds[variable].mean(dim=('lat', 'lon', 'plev'))
            # ds['means'] = ds[varname].mean(dim=('lat', 'lon', 'plev'))
        elif 'plev' in ds.coords and 'tau' in ds.coords:
            ds['mins'] = ds[variable].min(dim=('lat', 'lon', 'plev', 'tau'))
            ds['maxs'] = ds[variable].mean(dim=('lat', 'lon', 'plev', 'tau'))
            # ds['means'] = ds[varname].mean(dim=('lat', 'lon', 'plev', 'tau'))
        elif 'lat' not in ds.coords and 'lon' not in ds.coords:
            ds['maxs'] = ds[variable]
            ds['mins'] = ds[variable]
        else:
            ds['mins'] = ds[variable].min(dim=('lat', 'lon'))
            ds['maxs'] = ds[variable].mean(dim=('lat', 'lon'))
        # ds['vmax'] = xr.DataArray(np.zeros(ds['time'].size), coords=[ds['time']])
        # ds['vmin'] = xr.DataArray(np.zeros(ds['time'].size), coords=[ds['time']])
        vmax = np.zeros(ds['time'].size)
        # vmin = np.zeros(ds['time'].size)
        # s = datetime.now()
        # maxrolling = client.compute(ds['maxs'].rolling({'time':12}).mean())
        # minrolling = client.compute(ds['mins'].rolling({'time':12}).mean()).result()

        if not only_plots:
            maxrollingstd = client.compute(ds['maxs'].rolling({'time':12}, min_periods=1).std()).result()
            maxrollingvar = client.compute(ds['maxs'].rolling({'time':12}, min_periods=1).mean()).result()
            ds = ds.compute()
            # e = datetime.now()
            # import ipdb; ipdb.set_trace()
            # minrollingstd = client.compute(ds['mins'].rolling({'time':12}).std()).result()

            num_segments = 10
            segments = list(
                range(0, ds['time'].size, ds['time'].size//num_segments))
            futures = []
            for idx, seg in enumerate(segments):
                if idx == num_segments - 1:
                    seg_end = ds['time'].size
                else:
                    seg_end = segments[idx + 1]

                temp_ds = ds['time'][seg: seg_end]
                chunk = ds.sel(time=temp_ds)
                futures.append(
                    client.submit(
                        run_chunk, 
                            chunk, 
                            dataset_id, 
                            maxrollingvar, 
                            maxrollingstd, 
                            (seg, seg_end), 
                            idx))

            for f in as_completed(futures):
                vx, issues, seg = f.result()
                vmax[seg[0]: seg[1]] = vx
        plot_minmaxmean(pngpath, ds, vmax, dataset_id, debug=debug)

    return issues


def verify_dataset(dataset_path, dataset_id, variable, output, only_plots=False, debug=False):

    issues = list()
    pngpath = os.path.join(output, "{}.png".format(dataset_id))
    return check_sq_variance(dataset_path, dataset_id, variable, pngpath, only_plots), dataset_id
