# Publication steps
* move the data

* generate map files

command format:

`esgmapfile make -i /path/to/ini/directory --max-processes <some_number_of_cores> --project <e3sm/cmip6> --outdir /output/location <path_to_data_directory>`

command example:

`esgmapfile make --debug -i /p/user_pub/work/E3SM/ini/ --max-processes 20 --project e3sm --outdir /p/user_pub/work/E3SM/mapfiles /p/user_pub/work/E3SM/1_0/1950-Control/0_25deg_atm_18-6km_ocean/atmos/720x1440/model-output/6hr_snap/ens1`




