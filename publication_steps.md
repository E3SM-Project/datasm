# Publication steps
#### move the data
Setup publication directory structure


#### generate map files:

The mapfile generation step is usually best to be run on acme1, since it has way more compute power then aims3. Make sure you have your publication environment setup

`conda activate pub`

command format:

`esgmapfile make -i /path/to/ini/directory --max-processes <some_number_of_cores> --project <e3sm/cmip6> --outdir /output/location <path_to_data_directory>`

command example:

`esgmapfile make --debug -i /p/user_pub/work/E3SM/ini/ --max-processes 20 --project e3sm --outdir /p/user_pub/work/E3SM/mapfiles /p/user_pub/work/E3SM/1_0/1950-Control/0_25deg_atm_18-6km_ocean/atmos/720x1440/model-output/6hr_snap/ens1`

#### Publication steps

This part *must* be run on aims3.llnl.gov once you log in, source the publication environment setup by the node admin.

`source /usr/local/conda/bin/activate esgf-pub`

#### generate auth cert

`myproxy-logon -s esgf-node.llnl.gov -l <your_esgf_user_name> -t 72 -o ~/.globus/certificate-file`

#### publish to ESGF

Run the publish.sh script pointing at the directory containg the previously generated map files




