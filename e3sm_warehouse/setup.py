"""
Setup for the E3SM e3sm_warehouse
"""
from setuptools import find_packages
from distutils.core import setup
from e3sm_warehouse import __version__

import distutils.cmd
import os

class CleanCommand(distutils.cmd.Command):
    """
    Our custom command to clean out junk files.
    """
    description = "Cleans out junk files we don't want in the repo"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        cmd_list = dict(
            DS_Store="find . -name .DS_Store -print0 | xargs -0 rm -f;",
            pyc="find . -name '*.pyc' -exec rm -rf {} \;",
            empty_dirs="find ./pages/ -type d -empty -delete;",
            build_dirs="find . -name build -print0 | xargs -0 rm -rf;",
            dist_dirs="find . -name dist -print0 | xargs -0 rm -rf;",
            egg_dirs="find . -name *.egg-info -print0 | xargs -0 rm -rf;"
        )
        for key, cmd in cmd_list.items():
            os.system(cmd)

setup(
    name="e3sm_warehouse",
    version=__version__,
    author="Sterling Baldwin and Anthony Bartoletti",
    author_email="baldwin32@llnl.gov",
    description="Automated Data Warehouse for E3SM Processing and Publication",
    entry_points={
        'console_scripts':
            ['e3sm_warehouse = e3sm_warehouse.__main__:main' ]},
    packages=find_packages(),
    package_dir={'e3sm_warehouse': 'e3sm_warehouse'},
    include_package_data=True,
    cmdclass={
        'clean': CleanCommand,
    })