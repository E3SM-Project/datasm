# So you've got a new CMIP6 variable


# Step, The First

You've probably recieved an email from a friendly scientist saying something along the lines of, "Hey, there's this handy CMIP6 variable that I need for my analysis, could you publish it?" The first thing to do is create a new entry in the E3SM variable conversion confluence page [here](https://acme-climate.atlassian.net/wiki/spaces/ED/pages/858882132/CMIP6+data+conversion+tables). Identify the CMIP6 table that the variable belongs to, and add an entry in the appropriate table. If the scientist helpfully included a conversion formula, include that as well, otherwise reach out to the appropriate science group leader (land, atmos, ocean) and ask them if they can supply the conversion formula as well as a scientist to perform a quality control check on the data.

**case study: snw**

    New CMIP variable request:

    SNOWICE variable.  For comparison I’d like to have corresponding output from the coupled (historical) 
    and AMIP simulations if that’s available (monthly frequency is fine). …the official CMIP6 name for this 
    variable is ‘snw’ and it’s part of the ‘landice’ realm, but there are no landice realm variables listed 
    for any of the E3SM models.  I think this corresponds to the variable ’SNOWICE’ in your model, but it 
    would be great to confirm that as well. 

### part 1: 
check the cmip6 metadata tables for the snw variable, keeping in mind the email said "it’s part of the ‘landice’ realm" so its probablt the LImon table instead of the Lmon table like the rest of the land data. Search the repo for your variable until you find  the [matching CMIP6 table enty](https://github.com/PCMDI/cmip6-cmor-tables/blob/master/Tables/CMIP6_LImon.json#L539).  You're going to have to track down the variable, so I suggest keeping a copy of the [cmip6 tables repository](https://github.com/PCMDI/cmip6-cmor-tables) on hand. Looking at the CMIP6_LImon.json we see the entry for our variable:

    "snw": 
        {
            "frequency": "mon", 
            "modeling_realm": "landIce land", 
            "standard_name": "surface_snow_amount", 
            "units": "kg m-2",
            "cell_methods": "area: mean where land time: mean", 
            "cell_measures": "area: areacella", 
            "long_name": "Surface Snow Amount", 
            "comment": "The mass of surface snow on the land portion of the grid cell divided by the land area in the grid cell; reported as missing where the land fraction is 0; excludes snow on vegetation canopy or on sea ice.", 
            "dimensions": "longitude latitude time", 
            "out_name": "snw", 
            "type": "real", 
            "positive": "", 
            "valid_min": "", 
            "valid_max": "", 
            "ok_min_mean_abs": "", 
            "ok_max_mean_abs": ""
        },

of particular interest here are the **comment** and **dimensions**, the first because it tells us about the meaning of the thing, and the second because it tells us we're dealing with a fairly simple 2d monthly land variable and this should be easy. 

### part 2:
Afte checking with the land scientists, it was discovered that although the requesting scientist thought they knew what the source variable was, they were wrong, and that the correct E3SM variable is H2OSNO **not** SNOWICE.

Now lets check the E3SM variable and see what its attributes are. Use the handy ESGF [metagrid search](https://esgf-dev1.llnl.gov/metagrid/search) or the old CoG seach (or look on your filesystem) to find the raw input data of interest, in this case the historical and amip ensembles from the E3SM-1-0 model version. 


    ~~> ncdump -h ~/Data/20181217.CNTL_CNPCTC1850_OIBGC.ne30_oECv3.edison.clm2.h0.1850-01.nc |  grep H2OSNO
            float H2OSNO(time, lndgrid) ;
                H2OSNO:long_name = "snow depth (liquid water)" ;
                H2OSNO:units = "mm" ;
                H2OSNO:cell_methods = "time: mean" ;
                H2OSNO:_FillValue = 1.e+36f ;
                H2OSNO:missing_value = 1.e+36f ;

In this case the E3SM units are `H2OSNO:units = "kg/m2" ;`, and the CMIP6 units are `"units": "kg m-2",`, which although they look like they dont match, due to the wonders of the SI unit system "mm" == "kg m-2" when working with water variables and so no unit conversion is required.


### part 3:
Now lets go to the internal confluence table and make a new entry with the info we've discovered. Document the new formula and notify the people of interest.


## Step, The Second
Create a new branch of the [e3sm_to_cmip](https://github.com/E3SM-Project/e3sm_to_cmip/) repository to hold the new converter. If its a "simple" converter, i.e. is a 1-to-1 conversion from an E3SM variable to a CMIP6 variable (with perhaps a unit conversion) then this step is easy, simply add an entry in the [default handler specification](https://github.com/E3SM-Project/e3sm_to_cmip/blob/master/e3sm_to_cmip/resources/default_handler_info.yaml). Supported unit conversions are: 

    'g-to-kg' -> data / 1000
    '1-to-%'  -> data * 100.0
    'm/s-to-kg/ms' -> data * 1000
    '-1' -> data * -1

You can add additional unit conversions [here](https://github.com/E3SM-Project/e3sm_to_cmip/blob/b69189eb25d0a533345aee322194d11e978b0f2a/e3sm_to_cmip/default.py#L12)

If the new variable does not have a simple one to one formula, you're going to have to create a new conversion handler. Follow one of the many examples [here](https://github.com/E3SM-Project/e3sm_to_cmip/tree/master/e3sm_to_cmip/cmor_handlers). 

**case study: snw**

### part 1:
In part 1 we identified that this was a simple handler, so this should be fairly easy. First lets make a new branch

    >> git checkout -b new-handler-snw
    Switched to a new branch 'new-handler-snw'

now all we need to do is add an entry in the [default handlers file](https://github.com/E3SM-Project/e3sm_to_cmip/blob/master/e3sm_to_cmip/resources/default_handler_info.yaml) 

    - cmip_name: snw
      e3sm_name: H2OSNO
      units: 'kg m-2'
      table: CMIP6_LImon.json

looking pretty good, lets create some sample data so we can run it. 

    >> ncclimo -7 --dfl_lvl=1 --no_cll_msr  -v H2OSNO  -s 1 -e 1 -o $Data/tmp/ --map=$Data/map_ne30np4_to_cmip6_180x360_aave.20181001.nc  -O $Data/timeseries --ypf=10 -i $Data/land/native/model-output/mon/ens1/v0 --sgs_frc=$Data/land/native/model-output/mon/ens1/v0/20180129.DECKv1b_piControl.ne30_oEC.edison.clm2.h0.0001-01.nc/landfrac

    Started climatology splitting at Wed Aug 11 16:43:25 PDT 2021
    Running climatology script ncclimo from directory /home/baldwin32/anaconda3/envs/warehouse/bin
    NCO binaries version 4.9.9 from directory /home/baldwin32/anaconda3/envs/warehouse/bin
    Parallelism mode = background
    Timeseries will be created for only one variable
    Will split data for each variable into one timeseries of length 1 years
    Splitting climatology from 12 raw input files in directory /land/native/model-output/mon/ens1/v0
    Each input file assumed to contain mean of one month
    Native-grid split files to directory /land/180x360/time-series/mon/ens1/v0-tmp
    Regridded split files to directory /land/180x360/time-series/mon/ens1/v0
    Wed Aug 11 16:43:26 PDT 2021: Generated /land/180x360/time-series/mon/ens1/v0-tmp/H2OSNO_000101_000112.nc
    Input #00: /land/180x360/time-series/mon/ens1/v0-tmp/H2OSNO_000101_000112.nc
    Map/Wgt  : /maps/map_ne30np4_to_cmip6_180x360_aave.20181001.nc
    Wed Aug 11 16:43:27 PDT 2021: Regridded /land/180x360/time-series/mon/ens1/v0/H2OSNO_000101_000112.nc
    Quick plots of last timeseries segment of last variable split:
    ncview /land/180x360/time-series/mon/ens1/v0/H2OSNO_000101_000112.nc &
    panoply /land/180x360/time-series/mon/ens1/v0/H2OSNO_000101_000112.nc &
    Completed 1-year climatology operations for input data at Wed Aug 11 16:43:27 PDT 2021
    Elapsed time 0m2s

### part 2:
With this new regridded timeseries we can take the converter for a run and see how it goes. For this you will need a working installation of the [e3sm_to_cmip](https://github.com/E3SM-Project/e3sm_to_cmip/) package, as well as the [cmip6 metadata tables](https://github.com/PCMDI/cmip6-cmor-tables)

    >> python -m e3sm_to_cmip -i $Data/land/180x360/time-series/mon/ens1/v0 -o $Data -t ~/projects/cmip6-cmor-tables/Tables/ -u piControl_r1i1p1f1.json -v snw --mode lnd

    [*] Writing log output to: /p/user_pub/e3sm/baldwin32/warehouse_testing/converter.log
    [+] Running CMOR handlers in parallel
    100%|█████████████████████████████████████████████████████████████████| 1/1 [00:00<00:00,  1.08it/s]
    [+] 1 of 1 handlers complete


## Step, The Third
Once you're able to produce the variable manually using the e3sm_to_cmip package, supply a sample of the variable output to the responsible scientist for quality assurance. Its best to supply them with a 5 year file so there's enough data to do a thurough check. If they give you the green light then merge your changes into the e3sm_to_cmip package and create a new version tag. 

## Step, The Fourth
Now that the convertsion handler is working and merged, you can update the warehouse dataset specification to include the new variable. Under esgfpub/warehouse/warehouse/resources/ open the dataset_spec.yaml file. There are two top level objects in the file, "tables" and "project," the first thing that needs to change is for the new variable to be added to the appropriate place under "tables." If its an Amon variable, add it to the variable list under Amon, etc. By default, anything that shows up in those tables will now be included in the CMIP6 datasets for ALL CASES. If the raw E3SM input variable isnt included in any of the cases, then this new variabler should NOT be included for the case. You will need to add the variable to all the case sections under the "except" section, you can see an example [here](https://github.com/E3SM-Project/esgfpub/blob/master/warehouse/warehouse/resources/dataset_spec.yaml#L262)

Merge this new change into the 'master' branch and install the new change locally.

## Step, The Fifth
You can now envoke the warehouse to create your new CMIP6 datasets! This should be as simple as running `warehouse postprocess -d CMIP6.*.<YOUR_NEW_VARIABLE>.*` and then after the datasets are produced run `warehouse auto -d CMIP6.*.<YOUR_NEW_VARIABLE>.*` which should publish them. Its advised that you run a single case first before envoking the run-everything command, as any problems will be easier to solve with a single case then when working with all the cases at once.