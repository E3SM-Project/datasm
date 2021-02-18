"""
Setup for the E3SM warehouse
"""
from setuptools import find_packages, setup
from warehouse.version import __version__

setup(
    name="warehouse",
    version=__version__,
    author="Sterling Baldwin",
    author_email="baldwin32@llnl.gov",
    description="Automated Data Warehouse for E3SM Processing and Publication",
    entry_points={
        'console_scripts':
            ['warehouse = warehouse.__main__:main' ]},
    packages=find_packages(),
    package_dir={'warehouse': 'warehouse'},
    include_package_data=True)
