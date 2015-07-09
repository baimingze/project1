#!/usr/bin/env python

# download files for ECA from Sourceforge

import os
import sys
import platform

if sys.version_info < (2, 6):
    error = "ERROR: Insilicos Cloud Army requires Python 2.6 or newer (but NOT Python 3)... exiting."
    print >> sys.stderr, error
    sys.exit(1)
	
isWindows = ( platform.system().startswith('Windows') ) 
installdir = "."
if ( len(sys.argv) > 1) :
    installdir = sys.argv[1]
print "installing to %s"%installdir

dataURL="http://ica.svn.sourceforge.net/svnroot/ica/branches/2.0/ensemble/"
if ( len(sys.argv) > 2 ) :
    dataURL = sys.argv[2]
print "downloading from %s"%dataURL

datafiles = [
    "src/distribute_setup.py",
    "src/setup.py",

    "data/covertype/covertype results summary.xlsx",
    "data/covertype/covtype.data.gz",
    "data/covertype/covtype.info",
    "data/jones/jones results summary.xlsx",
    "data/jones/jones.names",
    "data/jones/jones.test.gz",
    "data/jones/jones.train.gz",
    "data/satimage/sat.doc",
    "data/satimage/sat.trn.gz",
    "data/satimage/sat.tst.gz",

    "src/README",
    "doc/ECA_QuickStartGuide.pdf",
    "doc/ECA_UserManual.pdf",

    "LICENSE-2_0.txt",
    "src/bangbang.R",
    "src/eca_common_framework.R",
    "src/eca_launch_helper.py",
    "src/eca_launch_rmpi.py",
    "src/eca_rmpi_framework.R",
    "src/eca_script_template.R",
    "src/eca_mapreduce_framework.R",
    "src/eca_launch_mapreduce.py",
    "src/example_satimage.py",
    "src/example_jones.py",

    "src/decision_tree_jones_job_config.json",
    "src/decision_tree_satimage_job_config.json",
    "src/decision_tree_covertype_job_config.json",
    "src/neural_network_jones_job_config.json",
    "src/neural_network_single_jones_job_config.json",

    "src/eca_decision_forest.R",
    "src/eca_decision_forest_folds.R",
    "src/eca_neural_network_ensemble.R",
    "src/eca_sequential_framework.R",
    "src/general_config_template.json",
    
    "src/cluster_infrastructure/config_nfs_client.sh",
    "src/cluster_infrastructure/config_nfs_head.sh",
    "src/cluster_infrastructure/sshtest.sh",
    "src/cluster_infrastructure/config_node_for_ica.sh",
    "src/cluster_infrastructure/setup.py",
    "src/cluster_infrastructure/start_node.py",
    "src/cluster_infrastructure/start_node.sh",
    "src/README"

    ]


# now download files as needed
import urllib
for datafile in datafiles :
    localfile = installdir+"/"+datafile
    if not os.path.exists(os.path.split(localfile)[0]) :
        os.makedirs(os.path.split(localfile)[0])
    url = dataURL+datafile
    print "downloading "+url+" to "+localfile+"..."
    for retry in range( 3 ) :
        try:
            urllib.urlretrieve(url, localfile)
            if (not isWindows and localfile.endswith(".py")) :
                os.system("chmod a+x "+localfile)
            break
        except Exception, e :
            pass
if (not isWindows) :
	os.system("chmod -R a+wr "+installdir)
print "downloads complete."

