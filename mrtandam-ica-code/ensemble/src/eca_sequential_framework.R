#! /usr/bin/env Rscript

# framework to run a machine learning ensemble in R sequentially
#   on a single processor
# - designed to provide sequential baseline benchmark for RMPI 
#     implementation of Insilicos Ensemble Cloud Army
# - produced by modification of RMPI implementation
# - use in place of standard eca_rmpi_framework.R by specifying in job
#     config file: "RMPI_FrameworkScriptFileName": "eca_sequential_framework.R"

#  Copyright (C) 2011 Insilicos LLC  All Rights Reserved
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

numWorkers <<- 1

# working in the parent
ensembleSize <- as.integer(eca_config$ensembleSize)
# set a random seed just for stable regression tests
set.seed(1)

# pull in the user supplied canonical functions
eca_log( "source the implementation code ", implementation ) 
source( implementation )  # this is where the heavy lifting is

# and do the actual work
eca_log( "< common preamble >", timeStamp = FALSE )
common_preamble()       # run stuff common to client and head, if any

eca_log( "< start timer on ensemble execution >", timeStamp = FALSE )
ptm <- proc.time()      # start timer on ensemble execution

eca_log( "< client preamble >", timeStamp = FALSE ) 
client_preamble()
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
	numResults = numResults + 1
	head_resulthandler(client_ensemblecalc(numResults),numResults)
	if (numResults >= ensembleSize) {   # are we there yet? 
		break
	}
}

# cleanup
eca_log( " ", timeStamp = FALSE )
eca_log( "< client postscript >", timeStamp = FALSE )
client_postscript()
eca_log( "< head postscript >", timeStamp = FALSE )
head_postscript() # head cleanup, if any

eca_log( paste( "ensemble runtime = ", ( proc.time() - ptm )[ 3 ], " seconds", sep = "" ), timeStamp = FALSE ) 

# run cleanup stuff common to host and clients, if any
eca_log( " ", timeStamp = FALSE )
eca_log( "< common_postscript >", timeStamp = FALSE )
common_postscript() 

eca_log( "done!" )
