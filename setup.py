#!/usr/bin/env python

# Project skeleton maintained at https://github.com/jaraco/skeleton

# For python 2.x must use "setuptools==44.1.1"
# For python 3.6 must use "setuptools==51.0.0" and "setuptools_scm==6.4.2"

import io
import sys

import setuptools

with io.open('README.rst', encoding='utf-8') as readme:
    long_description = readme.read()

PY2 = sys.version_info[0] < 3
PY36 = sys.version_info[:2] == (3, 6)

name = 'scrunch'
description = 'Pythonic scripting library for cleaning data in Crunch'
nspkg_technique = 'native'
"""
Does this package use "native" namespace packages or
pkg_resources "managed" namespace packages?
"""

crunch_cube = "cr.cube"

if PY2:
    crunch_cube = "cr.cube==2.3.9"

params = dict(
    name=name,
    use_scm_version=True,
    author="Crunch.io",
    author_email="dev@crunch.io",
    description=description or name,
    long_description=long_description,
    url="https://github.com/Crunch-io/" + name,
    packages=setuptools.find_packages(),
    include_package_data=True,
    namespace_packages=(
        name.split('.')[:-1] if nspkg_technique == 'managed'
        else []
    ),
    python_requires='>=2.7',
    install_requires=[
        "pycrunch==0.5.5",
        "requests==2.27.0",
        crunch_cube,
        'six',
    ],
    extras_require={
        'testing': [
            # upstream
            "pytest==4.6.11",
            'collective.checkdocs',
            # 'pytest-flake8==2.18.4',

            # local
            'pytest-cov==2.12.1',
            'mock==3.0.5',
            'isodate',
            'backports.unittest_mock',
        ],
        'docs': [
            # upstream
            'sphinx',
            'jaraco.packaging>=3.2',
            'rst.linker>=1.9',

            # local
        ],
        'pandas': ['pandas'],
    },
    setup_requires=[
        'setuptools_scm>=1.15.0',
    ],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
    ],
    entry_points={
    },
)

if PY2:
    params["install_requires"].extend([
        "importlib_metadata==0.17",
        # Pin dependency versions to work with python2.7.
        "zipp==1.1.0",
        "tabulate==0.8.10",
        "configparser==4.0.2",
        "scipy==1.2.3",
        "numpy==1.13.3",
        "contextlib2==0.6.0",
        "certifi==2021.10.8"
    ])
elif PY36:
    params["install_requires"].extend([
        # Pin for python3.6
        # "platformdirs==2.4.0",
        "setuptools==51.0.0",
        "setuptools_scm==6.4.2",
        "scipy==1.1.0",
        "numpy==1.15.1",
        "dataclasses==0.8"
    ])

if __name__ == '__main__':
    setuptools.setup(**params)
