"""
Setup for esgfpub
"""
from setuptools import find_packages, setup

setup(
    name="esgfpub",
    version="0.1.1",
    author="Sterling Baldwin",
    author_email="baldwin32@llnl.gov",
    description="E3SM Automated publication to ESGF",
    entry_points={
        'console_scripts':
            ['esgfpub = esgfpub.__main__:main']},
    packages=find_packages())
