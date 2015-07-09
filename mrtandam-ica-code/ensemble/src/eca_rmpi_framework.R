#! /usr/bin/env Rscript

# framework to run a machine learning ensemble in R, using RMPI 
#   implementation of Insilicos Ensemble Cloud Army

#  Copyright (C) 2010 Insilicos LLC  All Rights Reserved
#  Original authors: Jeff Howbert, Natalie Tasman, Brian Pratt

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
# (looks like eca_config=list("ensembleSize"="12","numberOfClientNodes"="3", etc)
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

# get the name of the implementation script to be used with our framework
implementation = eca_config$scriptFileName
if ( ! file.exists( implementation ) ) {
	# probably because it's actually in the data dir after being copied off S3
	implementation = paste(eca_config$dataDir,implementation,sep="")
}

# use eca_log instead of cat or print so we can channel communications properly
glob_logstrs <- c("")

eca_log( "running..." )

# announce R version in debug mode
eca_debug( version )

if ( eca_runLocal() ) {
	numWorkers <<- 1
} else {
    library( "Rmpi" )                                       # initialize the Rmpi environment
    numWorkers <<- (( as.integer( eca_config$numberOfClientNodes ) * as.integer( eca_config$numberOfRTasksPerClient ) ) + (as.integer( eca_config$numberOfRTasksOnHead ) ))
    mpi.spawn.Rslaves( comm = 1, nslaves = numWorkers )     # spawn worker nodes
    if ( mpi.comm.size() < numWorkers + 1 )
	{
		eca_log( "Please initialize an MPI cluster of at least xx processors," )
		eca_log( "then, try again" )
	#    mpi.quit()
	}
}

# working in the parent
ensembleSize <- as.integer(eca_config$ensembleSize)
# set a random seed just for stable regression tests
set.seed(1)

# pull in the user supplied canonical functions
eca_log( "source the implementation code ", implementation ) 
source( implementation )  # this is where the heavy lifting is

if (! eca_runLocal() ) {
	# supply config and support functions to clients
	mpi.bcast.Robj2slave( eca_readpipe )
	mpi.bcast.Robj2slave( eca_read_datafile )
	mpi.bcast.Robj2slave( eca_construct_pipe_cmd )
	mpi.bcast.Robj2slave( eca_config )
	mpi.bcast.Robj2slave( eca_verbose )
	mpi.bcast.Robj2slave( eca_log )
	mpi.bcast.Robj2slave( eca_asPositiveInt )
	mpi.bcast.Robj2slave( eca_runLocal )
	mpi.bcast.Robj2slave( eca_dump_log )
	mpi.bcast.cmd( glob_logstrs <<- c("") )
	mpi.bcast.cmd( runningAsClient <<- TRUE )
	mpi.bcast.cmd( runningAsHadoopMapper <<- FALSE )
	# send user supplied canonical functions to clients
	mpi.bcast.Robj2slave( common_preamble ) # run stuff common to client and head, if any
	mpi.bcast.Robj2slave( client_preamble ) 
	mpi.bcast.Robj2slave( client_ensemblecalc ) 
	mpi.bcast.Robj2slave( client_postscript )
	mpi.bcast.Robj2slave( common_postscript )
}

# and do the actual work
eca_log( "< common preamble >", timeStamp = FALSE )
if ( ! eca_runLocal() ) {
	if (eca_verbose()) mpi.bcast.cmd( eca_log("running common_preamble...") )
	mpi.bcast.cmd( common_preamble() ) # run stuff common to client and head, if any
}
common_preamble()       # run stuff common to client and head, if any

eca_log( "< start timer on ensemble execution >", timeStamp = FALSE )
ptm <- proc.time()      # start timer on ensemble execution

eca_log( "< client preamble >", timeStamp = FALSE ) 
if (  eca_runLocal() ) {
	client_preamble()
} else {
	if (eca_verbose()) mpi.bcast.cmd( eca_log("running client_preamble...") )
	mpi.bcast.cmd( client_preamble() )
}
eca_log( "< head preamble >", timeStamp = FALSE ) 
head_preamble()         # run head setup, if any

# main loop
# we'll call the cluster enough times to get n=ensembleSize results
eca_log( "< running ensemble calculations >", timeStamp = FALSE )
eca_log( " ", timeStamp = FALSE )
numClustCalls = ( ensembleSize %/% numWorkers )  # integer division
if ((numClustCalls * numWorkers ) != ensembleSize) {
	numClustCalls = numClustCalls + 1
}
numResults = 0
for ( call in 1 : numClustCalls )
{
	if ( eca_runLocal() ) {
		numResults = numResults + 1
		head_resulthandler(client_ensemblecalc(numResults),numResults)
	} else {
		randSeedBase <<- ( call - 1 ) * numWorkers
		mpi.bcast.Robj2slave( randSeedBase ) # send new randSeedBase value to all workers
		if (eca_verbose()) mpi.bcast.cmd( eca_log(paste("running client_ensemblecalc(",randSeedBase+mpi.comm.rank(1),")...",sep="") ))
		# compiles the returned results as a list, with the indexes being the rank of the slave, and the value being the returned value. (http://math.acadiau.ca/ACMMaC/Rmpi/methods.html)
		returnObj <- mpi.remote.exec( client_ensemblecalc(randSeedBase+mpi.comm.rank(1)) ) # run the calculation on all workers
		for ( worker in 1 : numWorkers )
		{
			head_resulthandler(returnObj[[worker]],randSeedBase+worker) 
			numResults = numResults + 1 
			if (numResults >= ensembleSize) # are we there yet?
			{  
				break
			}
		}
	}
	if (numResults >= ensembleSize) {   # are we there yet? 
		break
	}
}

if( ! eca_runLocal() ) {
	mpi.remote.exec( eca_dump_log() )
}

# cleanup
eca_log( " ", timeStamp = FALSE )
eca_log( "< client postscript >", timeStamp = FALSE )
if ( eca_runLocal() ) {
	client_postscript()
} else {
	if (eca_verbose()) mpi.bcast.cmd( eca_log("running client_postscript...") )
	mpi.bcast.cmd( client_postscript() ) # client cleanup, if any
}
eca_log( "< head postscript >", timeStamp = FALSE )
head_postscript() # head cleanup, if any

eca_log( paste( "ensemble runtime = ", ( proc.time() - ptm )[ 3 ], " seconds", sep = "" ), timeStamp = FALSE ) 

# run cleanup stuff common to host and clients, if any
eca_log( " ", timeStamp = FALSE )
eca_log( "< common_postscript >", timeStamp = FALSE )
common_postscript() 
if (! eca_runLocal() ) {
	if (eca_verbose()) mpi.bcast.cmd( eca_log("running common_postscript...") )
	mpi.bcast.cmd( common_postscript() ) 
}

eca_log( "done!" )

if (! eca_runLocal() ) {
	if (eca_verbose()) {
		returnObj <- mpi.remote.exec( eca_dump_log() ) # all workers return accumulated logs
		eca_log("client logs:")
		print(returnObj) # these already have timestamps, don't call eca_log here
	}
	# close workers and exit
	mpi.close.Rslaves()
	mpi.quit( save = "no" )
}
