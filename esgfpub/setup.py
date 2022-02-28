"""
Setup for esgfpub
"""
from setuptools import find_packages, setup

from esgfpub import __version__

setup(
    name="esgfpub",
    version=__version__,
    author="Sterling Baldwin",
    author_email="baldwin32@llnl.gov",
    description="Automated publication tools for ESGF",
    entry_points={
        'console_scripts':
            ['esgfpub = esgfpub.__main__:main',
             'timecheck = esgfpub.scripts.timerect.timechecker:main',
             'timerect = esgfpub.scripts.timerect.timerectifier:main']},
    packages=find_packages(),
    package_dir={'esgfpub': 'esgfpub'},
    include_package_data=True)