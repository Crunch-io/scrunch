#!/usr/bin/env python
"""
Setup configuration for scrunch package.
Project skeleton maintained at https://github.com/jaraco/skeleton
"""

import io
import sys
from pathlib import Path

import setuptools

# Read long description from README
readme_path = Path(__file__).parent / "README.md"
with io.open(readme_path, encoding="utf-8") as readme:
    long_description = readme.read()

# Python version detection
PY_VERSION = sys.version_info[:2]
PY2 = sys.version_info[0] < 3
PY36 = PY_VERSION == (3, 6)
PY311 = PY_VERSION == (3, 11)

# Package metadata
PACKAGE_NAME = "scrunch"
DESCRIPTION = "Pythonic scripting library for cleaning data in Crunch"
AUTHOR = "Crunch.io"
AUTHOR_EMAIL = "dev@crunch.io"
URL = f"https://github.com/Crunch-io/{PACKAGE_NAME}"

# Version-specific dependencies
CRUNCH_CUBE = "cr.cube==2.3.9" if PY2 else "cr.cube==3.2.0"
REQUESTS = "requests==2.27.0" if PY2 else "requests"

# Base dependencies
install_requires = [
    "pycrunch==0.5.5",
    REQUESTS,
    CRUNCH_CUBE,
    "six",
]

# Python 2.7 specific dependencies
if PY2:
    install_requires = install_requires + [
        "importlib_metadata==0.17",
        "zipp==1.1.0",
        "tabulate==0.8.10",
        "configparser==4.0.2",
        "scipy==1.2.3",
        "numpy==1.13.3",
        "contextlib2==0.6.0",
        "certifi==2021.10.8",
    ]

# Python 3.6 specific dependencies
elif PY36 or PY311:
    install_requires = install_requires + [
        "setuptools==51.0.0",
        "setuptools_scm==6.4.2",
        "scipy==1.1.0",
        "numpy==1.15.1",
        "dataclasses==0.8",
    ]
# Python 3.11 specific dependencies
elif PY36 or PY311:
    install_requires = install_requires + [
        "setuptools",
        "setuptools_scm<=10.0.0",
    ]

# Testing dependencies
testing_requires = [
    "pytest",
    "collective.checkdocs",
    "pytest-cov==2.12.1",
    "mock==3.0.5",
    "isodate",
]

if not PY2:
    testing_requires = testing_requires + ["pyspssio"]

# Documentation dependencies
docs_requires = [
    "sphinx",
    "jaraco.packaging>=3.2",
    "rst.linker>=1.9",
]

# Setup configuration
setuptools.setup(
    name=PACKAGE_NAME,
    use_scm_version=True,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=URL,
    packages=setuptools.find_packages(),
    include_package_data=True,
    python_requires=">=2.7",
    install_requires=install_requires,
    extras_require={
        "testing": testing_requires,
        "docs": docs_requires,
        "pandas": ["pandas"],
    },
    setup_requires=[
        "setuptools_scm>=1.15.0",
    ],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.11",
    ],
    entry_points={},
)
