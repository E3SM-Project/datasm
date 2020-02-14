"""
Setup for esgfpub
"""
from setuptools import find_packages, setup

setup(
    name="esgfpub",
    version="0.1.2",
    author="Sterling Baldwin",
    author_email="baldwin32@llnl.gov",
    description="Automated publication tools for ESGF",
    entry_points={
        'console_scripts':
            ['esgfpub = esgfpub.__main__:main']},
    packages=['esgfpub', 'esgfpub.util', 'esgfpub.publiction_checker'],
    package_dir={'esgfpub': 'esgfpub'},
    include_package_data=True)
