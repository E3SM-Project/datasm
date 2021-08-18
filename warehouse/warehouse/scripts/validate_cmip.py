import sys
import os
import argparse

import warnings
warnings.simplefilter('ignore')

from mpl_toolkits.basemap import Basemap
from matplotlib import pyplot as plt
import statsmodels.api as sm
import xarray as xr
import numpy as np

def plot_global(dataset, variable, pngpath, dataset_id, debug=False):
    
    m = Basemap(projection="eck4",lon_0=0,resolution='c')
    m.drawcoastlines()
    m.fillcontinents(color='coral',lake_color='aqua')
    m.drawparallels(np.arange(-90.,120.,30.))
    m.drawmeridians(np.arange(0.,360.,60.))
    m.drawmapboundary(fill_color='aqua')
    ny = dataset[variable].shape[0]
    nx = dataset[variable].shape[1]
    lons, lats = m.makegrid(nx, ny)
    x, y = m(lons, lats)
    m.contourf(x, y, dataset[variable])
    plt.title(variable)
    plt.plot(variable)
    plt.savefig(pngpath, dpi=100)

def plot_seasonal_decomp(dataset_path, dataset_id, outpath, debug=False):

    contents = os.listdir(dataset_path)
    if not contents:
        raise ValueError(f"Empty dataset directory {dataset_path}")
    
    # open the multi-file dataset, combining the files by their coords
    ds = xr.open_mfdataset(f'{dataset_path}/*.nc', combine='by_coords')

    variable = dataset_id.split('.')[-2]
    pngpath = f"{outpath}{os.sep}{dataset_id}.png"

    # if the variable doesn't have a time axis, just plot whatever's there
    if 'time' not in ds[variable].coords and 'time' not in ds[variable].dims:
        plot_global(ds, variable, pngpath, debug)
        return []

    # find the dimensions to average over
    possible_dims = ['depth', 'lat', 'lon', 'plev', 'tau', 'lev', 'sector']    
    dims = tuple(x if x != 'sector' else 'basin' for x in ds.dims if x in possible_dims)

    meanvalues = ds[variable].mean(dims).compute()
    mpd = meanvalues.to_pandas()

    # compute the seasonal decomposition
    decomposition = sm.tsa.seasonal_decompose(mpd, model='additive', period=12)

    # produce the plot
    fig, axes = plt.subplots(3)
    plt.suptitle(dataset_id)

    fig.set_size_inches(15, 10)
    fig.tight_layout(pad=5.0)

    xtick_positions = [i for i, t in enumerate(ds['time']) if i % (120) == 0]
    xtick_values = [t.values.item().strftime('%Y') for i, t in enumerate(ds['time']) if i % (120) == 0]
    # plt.xticks(xtick_positions, xtick_values)
    plt.setp(axes, xticks=xtick_positions, xticklabels=xtick_values)
    plt.xlabel('year')

    # top plot is global mean
    axes[0].set_title(f'global {variable} mean')
    axes[0].plot(meanvalues)

    # second plot is seasonality - mean
    trend = decomposition.trend[:].to_xarray()
    axes[1].set_title('trend')
    axes[1].plot(trend)

    # thrid plot is the residual
    resid = decomposition.resid[:].to_xarray()
    axes[2].set_title('residual')
    axes[2].plot(resid, 'o')

    plt.savefig(pngpath, dpi=100)

    rstd = np.std(resid)
    rmean = np.mean(resid)
    diff = rstd * 5
    # flag anything thats outside 5x away from the std as a potential issue
    issues = [f"{variable} - {r.time.values.item().strftime('%Y-%m')}" for r in resid if r > rmean + diff or r < rmean - diff]
    if issues:
        print("Potential issues found")
        for issue in issues:
            print(issue)
    return 0


def main():
    p = argparse.ArgumentParser(description="Generate plots to verify CMIP datasets")
    p.add_argument('input', help="Path to dataset directory, the directory should be filled with netCDF files that belong to the dataset")
    p.add_argument('output', help="Path to where generated plots should be saved")
    p.add_argument('dataset_id', help="The dataset_id of the variable being validated")
    args = p.parse_args()
    plot_seasonal_decomp(args.input, args.dataset_id, args.output)
    return 0

    
if __name__ == "__main__":
    
    sys.exit(main())