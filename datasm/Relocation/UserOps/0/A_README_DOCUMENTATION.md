This directory is intended to serve two goals.

1. It should contain symlinks to the "README" docs in each of the other Operations directories.
2. It should contain scripts that serve to produce documentation where possible.

The philosophy:

    Online resources like Confluence and Github are great places to read documentation.
    However, when it comes to formal data such as tables of variables, lists of required
    dataset types, or ANYTHING that can help serve to automate processing, such information
    SHOULD NOT be defined in Confluence, and then manually replicated on machines where
    it serves to guide processing.  Avoid unnecessary and error-prone labors.

    Such "definitional" and "control" data should be defined on Github, accessed automatically
    on machines where it will be used to guide processing, and moreover plied with scripts
    HERE, where possible, to produce the tables of values that can then be supplied to
    Confluence pages.

    There should be ONLY ONE source of important control data.

    In this regard, the tables that appear in the following Confluence pages should be
    converted to CSV or similar data, placed into an appropriate Github repository, and
    subsequently downloaded and converted to Excel tables, and uploaded to Confluence
    as "Excellable" tables:

        https://acme-climate.atlassian.net/wiki/spaces/DOC/pages/858882132/CMIP6+data+conversion+tables
            https://acme-climate.atlassian.net/wiki/spaces/DOC/pages/925304036/Amon+variable+conversion+table
            https://acme-climate.atlassian.net/wiki/spaces/DOC/pages/925402155/CFmon+variable+conversion+table
            https://acme-climate.atlassian.net/wiki/spaces/DOC/pages/925500501/Lmon+variable+conversion+table
            https://acme-climate.atlassian.net/wiki/spaces/DOC/pages/925597751/Omon+variable+conversion+table
            https://acme-climate.atlassian.net/wiki/spaces/DOC/pages/925597759/SImon+variable+conversion+table
            https://acme-climate.atlassian.net/wiki/spaces/DOC/pages/1010402171/fx+Ofx+variable+conversion+table
            https://acme-climate.atlassian.net/wiki/spaces/DOC/pages/2030927991/ATM+daily+and+3+hourly
            https://acme-climate.atlassian.net/wiki/spaces/DOC/pages/3079372872/LImon+variable+conversion+table

        https://acme-climate.atlassian.net/wiki/spaces/DOC/pages/1161691980/Default+Set+of+Model+Output+for+ESGF+publication

    Moreover, as an adjunct to the downloads, the values should be "installed" where E3SM Operational
    processing would employ them (e.g. [STAGING_RESOURCE], etc).
