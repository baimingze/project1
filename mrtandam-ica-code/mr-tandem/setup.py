#!/usr/bin/env python
import os
import sys

if sys.version_info < (2, 6):
    error = "ERROR: MR-Tandem requires Python 2.6+ ... exiting."
    print >> sys.stderr, error
    sys.exit(1)

# in case setuptools isn't already present	
import distribute_setup
distribute_setup.use_setuptools()

try:
    from setuptools import setup, find_packages

# README = open('ECA_QuickStartGuide.pdf').read()

setup(
    name='MR-Tandem',
    version='2.0',
    package_data={},
    packages=find_packages(),
    scripts=[],
    license='Apache',
    author='Insilicos LLC',
    author_email='info@insilicos.com',
    url="http://www.insilicos.com",
    description="MR-Tandem uses Hadoop to run the X!Tandem search engine "
    "on Amazon's Elastic Compute Cloud (EC2).",
    # long_description=README,
    classifiers=[
        'Environment :: Console',
        'Development Status :: 2 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'Intended Audience :: Other Audience',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache '
        'License (LGPL)',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Topic :: Education',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Clustering',
    ],
    install_requires=["boto>=2.1.0", "simplejson>=2.1.6"]
)
