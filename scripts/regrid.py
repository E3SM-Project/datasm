import os
import sys
from subprocess import Popen, PIPE
from multiprocessing import Pool

def regrid(names):
    inname, outname = names
    cmd = f"ncks -t 1 -O --fl_fmt=netcdf4_classic --no_tmp_fl --dfl_lvl=1 --hdr_pad=10000  --map=/export/zender1/data/maps/map_ne30np4_to_cmip6_180x360_aave.20181001.nc  --rgr no_stagger atm-timeseries/{inname} atm-timeseries/{outname}".split()
    out, err = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()
    out = out.decode('utf-8')
    err = err.decode('utf-8')
    if out or err:
        print(f"Error during regridding for {outname}")
        print(out, err)
    else:
        print(f"{outname} complete")

def main():
    variables = "TREFHT,TS,PS,PSL,U10,QREFHT,PRECC,PRECL,PRECSC,PRECSL,QFLX,TAUX,TAUY,LHFLX,CLDTOT,SHFLX,CLOUD,CLDLOW,CLDMED,CLDHGH,CLDICE,TGCLDIWP,TGCLDCWP,RELHUM,FSNTOA,PHIS,LWCF,SWCF,TMQ,FLUTC,FLUT,FSDSC,SOLIN,FSUTOA,FSUTOAC,FLNS,FSNS,FLNSC,FSNT,FLNT,FSDSC,FSNSC,FLDS,FSDS,T,U,V,OMEGA,Z3,Q,O3".split(',')
    inpath = "/p/user_pub/e3sm/baldwin32/workshop/piControl/atm-timeseries"
    files = os.listdir(inpath)

    names = []
    with Pool(6) as pool:
        for var in variables:
            inname = f"{var}_000101_050012.nc"
            outname = f"{var}_000101_050012_cmip6_180x360_aave.nc"
            if outname in files:
                print(f"Found {outname} skipping {var}")
                continue
            names.append((inname, outname))
        pool.map(regrid, names)

if __name__ == "__main__":
    main()