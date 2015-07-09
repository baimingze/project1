# build a release package for SourceForge
import os
import re
import zipfile

os.system('"C:\Program Files (x86)\NSIS\MakeNSIS.exe" installer\ECA_installer.nsi')

# find these values in the nsis file
PRODUCT_MAJOR_VERSION_NUMBER = -1
PRODUCT_MINOR_VERSION_NUMBER = -1
PRODUCT_REV_VERSION_NUMBER = -1
f = file('installer\ECA_installer.nsi')
for line in f:
	m = re.search('(?<=define PRODUCT_MAJOR_VERSION_NUMBER )\w+', line)
	if None != m:
		PRODUCT_MAJOR_VERSION_NUMBER = m.group(0)
	m = re.search('(?<=define PRODUCT_MINOR_VERSION_NUMBER )\w+', line)
	if None != m:
		PRODUCT_MINOR_VERSION_NUMBER = m.group(0)
	m = re.search('(?<=define PRODUCT_REV_VERSION_NUMBER )\w+', line)
	if None != m:
		PRODUCT_REV_VERSION_NUMBER = m.group(0)		

outfile = 'ECA_%s.%s.%s.zip'%(PRODUCT_MAJOR_VERSION_NUMBER,PRODUCT_MINOR_VERSION_NUMBER,PRODUCT_REV_VERSION_NUMBER)
myzip = zipfile.ZipFile(outfile, 'w')
myzip.write('README.txt')
myzip.write('doc/ECA_QuickStartGuide.pdf','ECA_QuickStartGuide.pdf')
myzip.write('ECA_Setup_%s.%s.%s.exe'%(PRODUCT_MAJOR_VERSION_NUMBER,PRODUCT_MINOR_VERSION_NUMBER,PRODUCT_REV_VERSION_NUMBER))
myzip.write('installer/setup_eca.sh','setup_eca.sh')

print("")
print("release file written as %s"%outfile)
print("contents:")
myzip.printdir()
