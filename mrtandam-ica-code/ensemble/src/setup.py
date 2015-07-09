#!/usr/bin/env python
import os
import sys

if sys.version_info < (2, 6):
    error = "ERROR: Insilicos Cloud Army requires Python 2.6 or newer (but NOT Python 3)... exiting."
    print >> sys.stderr, error
    sys.exit(1)

# in case setuptools isn't already present
import platform
import distribute_setup
distribute_setup.use_setuptools()

# for windows, download a prebuilt binary for pycrypto if not already there
isWin = ( platform.system().startswith('Windows') )
isWin64 = isWin and ( "64bit" == platform.architecture()[0] ) 
paramiko_require = "paramiko>=1.7.6"
if isWin64:
	paramiko_require = "paramiko==1.7.6"

try:
	import Crypto.PublicKey
except ImportError, e:
	pass
	if isWin : # should be able to install from source on linux
		print "installing windows binary for PyCrypto (needed for ssh use)..."
		import urllib
		import subprocess
		try :
			# for 64 bit use only paramiko 1.7.6, this allows us to use pycrypto 2.0.1 which has a 64bit binary available
			pycryptoinstall = ""
			if isWin64 :
				pycryptoinstallexe = "pycrypto-2.0.1.win-amd64-py2.6.exe"
				pycryptourl = "http://www.voidspace.org.uk/downloads/"+pycryptoinstallexe
				print "downloading "+pycryptourl+"..."
				urllib.urlretrieve(pycryptourl, pycryptoinstallexe)
				pycryptoinstall = pycryptoinstallexe+' /quiet /norestart'
			else :
				pycryptobase = "pycrypto-2.3.win32-py%d.%d" % (sys.version_info[0],sys.version_info[1])
				pycryptozip = pycryptobase+".zip"
				pycryptourl = "http://www.voidspace.org.uk/downloads/"+pycryptozip
				pycryptoinstall = "msiexec /i %s.msi /qn" % pycryptobase
				print "downloading "+pycryptourl+"..."
				urllib.urlretrieve(pycryptourl, pycryptozip)
				import os,zipfile
				print "opening %s..." % pycryptozip
				z = zipfile.ZipFile(pycryptozip,'r')
				z.extractall()
				z.close()
			print "installing pycrypto..."
			print pycryptoinstall
			subprocess.call(pycryptoinstall)
		except Exception, e:
			print "error installing pycrypto binary for ssh use: "
			print e
			exit(1)
	
# and for 64 bit windows, ridiculous contortions to avoid trying to compile C
# reference http://bugs.python.org/issue6792
json_require = "simplejson>=2.1.6"
if isWin64 : 
	try:
		import urllib
		import subprocess
		import tarfile
		jsonver="2.3.0"
		sjson = "simplejson-"+jsonver
		localfile= sjson+".tar.gz"
		urllib.urlretrieve("http://pypi.python.org/packages/source/s/simplejson/"+localfile,localfile)
		tf = tarfile.open(localfile,'r:gz')
		tf.extractall()
		os.chdir(sjson)
		import re
		o = open("mysetup.py","w")
		data = open("setup.py").read()
		o.write( re.sub("not IS_PYPY","False",data)  ) # so install runs without trying to compile C code
		o.close()		
		if (sys.argv[0].endswith("setup.py")) :
			subprocess.call(["mysetup.py","install"],shell=True)
		else : # explicit python invocation
			subprocess.call([sys.argv[0],"mysetup.py","install"],shell=True)
		json_require = "simplejson=="+jsonver
		
	except Exception, e :
		print "error installing python simplejson library: "
		print e
		exit(1)
	
from setuptools import setup, find_packages

# README = open('ECA_QuickStartGuide.pdf').read()

setup(
    name='Insilicos Ensemble Cloud Army',
    version='2.0',
    package_data={},
    packages = find_packages(),
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
    install_requires=[json_require , paramiko_require, "boto>=2.1.0" ]
)


