"""The setup script."""
import distutils.cmd
import os
from distutils.core import setup

from setuptools import find_packages


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
            egg_dirs="find . -name *.egg-info -print0 | xargs -0 rm -rf;",
        )
        for key, cmd in cmd_list.items():
            os.system(cmd)


setup(
    author="datasm developers",
    python_requires=">=3.9,<3.12",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
    ],
    description="A cli utility to automate complex nested workflows for publishing E3SM data to ESGF.",
    license="MIT License",
    include_package_data=True,
    keywords=["datasm"],
    name="datasm",
    packages=find_packages(include=["datasm", "datasm.*"]),
    package_dir={"datasm": "datasm"},
    entry_points={
        "console_scripts": [
            "datasm = datasm.__main__:main",
        ],
    },
    test_suite="tests",
    url="https://github.com/E3SM-Project/datasm",
    version="0.1.0",
    zip_safe=False,
    cmdclass={
        "clean": CleanCommand,
    },
)
