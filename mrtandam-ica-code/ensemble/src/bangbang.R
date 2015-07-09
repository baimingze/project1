#! /usr/bin/env Rscript
#
# An example of using an Insilicos Ensemble Cloud Army MPI cluster for
# other purposes - in this case, running the X!!Tandem peptide search engine
#(see the mr-tandem directory in this same SoureceForge project for the code
# that uses this)
#
# Copyright (C) 2010 Insilicos LLC  All Rights Reserved
# Original Authors Jeff Howbert, Natalie Tasman, Brian Pratt

#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


# read the R-formatted configuration data
# (looks like eca_config=list("ensembleSize"="12","numberOfClientNodes"="3", ... )
# should contain numberOfClientNodes, ensembleSize, sharedFile_trainingData, sharedFile_testData
args <- commandArgs( TRUE )
runningAsClient <<- FALSE
runningAsHadoopMapper <<- FALSE

source( args[ 1 ] )

# get API code 
frameworkSupportScript = eca_config$frameworkSupportScript
if ( ! file.exists( frameworkSupportScript ) ) {
	# probably because it's actually in the data dir after being copied off S3
	frameworkSupportScript = paste(eca_config$dataDir,frameworkSupportScript,sep="")
}

# get the framework support functions
source( frameworkSupportScript )
eca_log( "source the framework support functions in ", frameworkSupportScript )

# install bbtandem from our public S3 site
cmd = "cd /mnt/nfs-shared ; if [ ! -e bbtandem ]; then wget http://insilicos-public.s3.amazonaws.com/tandem_arch.sh ; wget --progress=dot:mega http://insilicos-public.s3.amazonaws.com/$(bash tandem_arch.sh mpi)/bbtandem ; chmod 777 bbtandem; fi"
eca_log( cmd )
system( cmd ) # downloads bbtandem

# scriptFileName is the xtandem params file
if ("True" == eca_config$oldSkool) { # run it in non-mpi fashion, for test and debug purposes
	cmd = paste("/mnt/nfs-shared/bbtandem ", eca_config$dataDir, eca_config$sharedFile_NoUpload_tandemParams,sep="")  
} else {
	taskCount <- ( as.integer( eca_config$numberOfRTasksOnHead )+(( as.integer( eca_config$numberOfClientNodes )) * as.integer( eca_config$numberOfRTasksPerClient ) ) )
	cmd = paste("mpiexec -machinefile  /mnt/nfs-shared/mpd.hosts -n ",taskCount," /mnt/nfs-shared/bbtandem -mpi ", eca_config$dataDir, eca_config$sharedFile_NoUpload_tandemParams,sep="")
}
eca_log( cmd )
system(cmd) 
eca_log( "done!" )


