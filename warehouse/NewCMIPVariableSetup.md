# So you've got a new CMIP6 variable


## Step The First

You've probably recieved an email from a friendly scientist saying something along the lines of, "Hey, there's this handy CMIP6 variable that I need for my analysis, could you publish it?" The first thing to do is create a new entry in the E3SM variable conversion confluence page [here](https://acme-climate.atlassian.net/wiki/spaces/ED/pages/858882132/CMIP6+data+conversion+tables). Identify the CMIP6 table that the variable belongs to, and add an entry in the appropriate table. If the scientist helpfully included a conversion formula, include that as well, otherwise reach out to the appropriate science group leader (land, atmos, ocean) and ask them if they can supply the conversion formula as well as a scientist to perform a quality control check on the data.

## Step The Second

Create a new branch of the [e3sm_to_cmip](https://github.com/E3SM-Project/e3sm_to_cmip/) repository to hold the new converter. If its a "simple" converter, i.e. is a 1-to-1 conversion from an E3SM variable to a CMIP6 variable (with perhaps a unit conversion) then this step is easy, simply add an entry in the [default handler specification](https://github.com/E3SM-Project/e3sm_to_cmip/blob/master/e3sm_to_cmip/resources/default_handler_info.yaml). Supported unit conversions are: 

    'g-to-kg' -> data / 1000
    '1-to-%'  -> data * 100.0
    'm/s-to-kg/ms' -> data * 1000
    '-1' -> data * -1

You can add additional unit conversions [here](https://github.com/E3SM-Project/e3sm_to_cmip/blob/b69189eb25d0a533345aee322194d11e978b0f2a/e3sm_to_cmip/default.py#L12)

If the new variable does not have a simple one to one formula, you're going to have to create a new conversion handler. Follow one of the many examples [here](https://github.com/E3SM-Project/e3sm_to_cmip/tree/master/e3sm_to_cmip/cmor_handlers).

## Step The Third

Once you're able to produce the variable manually using the e3sm_to_cmip package, supply a sample of the variable output to the responsible scientist for quality assurance. Its best to supply them with a 5 year file so there's enough data to do a thurough check. If they give you the green light then merge your changes into the e3sm_to_cmip package and create a new version tag. 

## Step The Fourth

Now that the convertsion handler is working and merged, you can update the warehouse dataset specification to include the new variable. Under esgfpub/warehouse/warehouse/resources/ open the dataset_spec.yaml file. There are two top level objects in the file, "tables" and "project," the first thing that needs to change is for the new variable to be added to the appropriate place under "tables." If its an Amon variable, add it to the variable list under Amon, etc. By default, anything that shows up in those tables will now be included in the CMIP6 datasets for ALL CASES. If the raw E3SM input variable isnt included in any of the cases, then this new variabler should NOT be included for the case. You will need to add the variable to all the case sections under the "except" section, you can see an example [here](https://github.com/E3SM-Project/esgfpub/blob/master/warehouse/warehouse/resources/dataset_spec.yaml#L262)

Merge this new change into the 'master' branch and install the new change locally.

## Step The Fifth

You can now envoke the warehouse to create your new CMIP6 datasets! This should be as simple as running `warehouse postprocess -d CMIP6.*.<YOUR_NEW_VARIABLE>.*` and then after the datasets are produced run `warehouse auto -d CMIP6.*.<YOUR_NEW_VARIABLE>.*` which should publish them. Its advised that you run a single case first before envoking the run-everything command, as any problems will be easier to solve with a single case then when working with all the cases at once.