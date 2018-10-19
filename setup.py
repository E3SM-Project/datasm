"""
Setup for esgfpub
"""
from setuptools import find_packages, setup

setup(
    name="esgfpub",
    version="0.0.2",
    author="Sterling Baldwin",
    author_email="baldwin32@llnl.gov",
    description="E3SM Automated publication to ESGF",
    scripts=["esgfpub"],
    packages=find_packages(
        exclude=["*.test", "*.test.*", "test.*", "test", "*_template.py"]))
