# download and install the latest R package, whatever that may be
# Copyright (C) Insilicos LLC 2011 All Rights Reserved

import urllib
import re
import subprocess

print "Obtaining current R version from http://cran.r-project.org/bin/windows/base/release.htm..."
f = urllib.urlopen("http://cran.r-project.org/bin/windows/base/release.htm")
redirectInfo = f.read()
# something like:
#<html>
#<head>
#<META HTTP-EQUIV="Refresh" CONTENT="0; URL=R-2.13.1-win.exe">
#<body></body>
exename = re.split('"',re.split("URL=",redirectInfo)[1])[0]
url = "http://cran.r-project.org/bin/windows/base/"+exename
print "Downloading R installer from "+url+"..."
urllib.urlretrieve(url,exename)
print "running R installer..."
subprocess.call([exename,"/SILENT"])
print "done"
