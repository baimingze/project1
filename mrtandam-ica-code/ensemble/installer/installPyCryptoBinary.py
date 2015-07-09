#!/usr/bin/env python
import os
import sys

if sys.version_info < (2, 6):
    error = "ERROR: Insilicos Cloud Army requires Python 2.6 or newer (but NOT Python 3)... exiting."
    print >> sys.stderr, error
    sys.exit(1)

try:
	import Crypto.PublicKey
	print "pyCrypto appears to be installed, good."
except ImportError, e:
	pass
	import platform
	if 'Windows' == platform.system() : # should be able to install from source on linux
		print "installing windows binary for PyCrypto (needed for ssh use)..."
		import urllib
		import subprocess
		try :
			# for 64 bit use only paramiko 1.7.6, this allows us to use pycrypto 2.0.1 which has a 64bit binary available
			pycryptoinstall = ""
			if "64bit" == platform.architecture()[0] :
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
				import zipfile
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
	
