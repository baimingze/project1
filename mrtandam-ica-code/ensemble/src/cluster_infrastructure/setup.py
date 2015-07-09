#!/usr/bin/env python
# installs the python packages needed on the cloud side of Ensemble Cloud Army
import os
import sys

if sys.version_info < (2, 6):
    error = "ERROR: Insilicos Cloud Army requires Python 2.6+ ... exiting."
    print >> sys.stderr, error
    sys.exit(1)

try:
    from setuptools import setup, find_packages
    extra = dict(install_requires=["boto==2.1.0",
                                   "simplejson==2.1.6"],
                 zip_safe=False)
except ImportError:
    import string
    from distutils.core import setup

    def convert_path(pathname):
        """
        Local copy of setuptools.convert_path used by find_packages (only used
        with distutils which is missing the find_packages feature)
        """
        if os.sep == '/':
            return pathname
        if not pathname:
            return pathname
        if pathname[0] == '/':
            raise ValueError("path '%s' cannot be absolute" % pathname)
        if pathname[-1] == '/':
            raise ValueError("path '%s' cannot end with '/'" % pathname)
        paths = string.split(pathname, '/')
        while '.' in paths:
            paths.remove('.')
        if not paths:
            return os.curdir
        return os.path.join(*paths)

    def find_packages(where='.', exclude=()):
        """
        Local copy of setuptools.find_packages (only used with distutils which
        is missing the find_packages feature)
        """
        out = []
        stack = [(convert_path(where), '')]
        while stack:
            where, prefix = stack.pop(0)
            for name in os.listdir(where):
                fn = os.path.join(where, name)
                if ('.' not in name and os.path.isdir(fn) and
                    os.path.isfile(os.path.join(fn, '__init__.py'))):
                    out.append(prefix + name)
                    stack.append((fn, prefix + name + '.'))
        for pat in list(exclude) + ['ez_setup', 'distribute_setup']:
            from fnmatch import fnmatchcase
            out = [item for item in out if not fnmatchcase(item, pat)]
        return out

    extra = {}

# README = open('ECA_QuickStartGuide.pdf').read()

setup(
    name='Insilicos Ensemble Cloud Army',
    version=2.0,
    packages=find_packages(),
    package_data={},
    scripts=[],
    license='Apache',
    author='Insilicos LLC',
    author_email='info@insilicos.com',
    url="http://www.insilicos.com",
    description="Ensemble Cloud Army is a utility for exploring Ensemble Learning "
    "methods hosted on Amazon's Elastic Compute Cloud (EC2).",
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
    **extra
)
