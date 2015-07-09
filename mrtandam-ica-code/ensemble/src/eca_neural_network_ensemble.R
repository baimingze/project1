# R functions to run an ensemble of neural networks, as directed by the 
#  RMPI framework for Insilicos Ensemble Cloud Army

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

# 
# section: code common to client and head modes
#

# runs just after config is loaded, on client and head node alike
common_preamble <- function() {
    library( "nnet" ) 
    # load training and test datasets
    # TODO: more general input handling (Weka ARFF or saved R dataframes)
    sharedFile_trainingData <- eca_config$sharedFile_GZ_trainingData
    sharedFile_testData <- eca_config$sharedFile_GZ_testData
    # note use of eca_read_datafile() to allow gzip handling, format recognition
    train <<- eca_read_datafile( sharedFile_trainingData )
    # note dimensions ( <<- instead of <- makes them globals)
    nTrainRows <<- dim( train )[ 1 ]
    nTrainCols <<- dim( train )[ 2 ]
    # TODO: deal with case where test has class values not present in train
    nClass <<- length( unique( train[ , nTrainCols ] ) )

    # note use of eca_read_datafile() to allow gzip handling, format recognition
    # TODO: check that train and test have same column count
    test <<- eca_read_datafile( sharedFile_testData )
    nTestRows <<- dim( test )[ 1 ]
    
    # form factor, then convert from factor values to factor indexes
    trainClassTrueFactor <<- as.factor( train[ , nTrainCols ] )
    trainClassTrue <<- as.numeric( trainClassTrueFactor )   
}

# last thing run, on both client and head nodes
common_postscript <- function() {
  dummy <- 0  # prevents output "NULL"
}

#
# section: client logic 
#

# code run by client before main loop
client_preamble <- function() 
{
  nHidden <<- as.integer( eca_config$numHidden )
  maxIter <<- as.integer( eca_config$maxIter )
  trainClassInd <<- class.ind( trainClassTrue )	        # create training set class indicator matrix
  dummy <- 0  # prevents output "NULL"
}

# accepts a unique ID integer (used here as a random seed), performs ensemble calc
client_ensemblecalc <- function( randSeedVal )
{
    set.seed( randSeedVal )

	# range-checked string to int conversion, understands "all" (=maxval)
    nFeatSub <- eca_asPositiveInt( eca_config$numberFeaturesInSubset, ( nTrainCols - 1 ) )
    featureSubset <- c( sample( 1 : ( nTrainCols - 1 ), nFeatSub ) )

	# range-checked string to int conversion, understands "all" (=maxval)
    nInstSub <- eca_asPositiveInt( eca_config$numberInstancesInSubset, nTrainRows )
	instanceSubset <- sample( 1 : nTrainRows, nInstSub )

	trainSub <- train[ instanceSubset, featureSubset ]
    trainClassIndSub <- trainClassInd[ instanceSubset, ]
	testSub <- test[ , featureSubset ]

	nWts <- ( nFeatSub + 1 ) * nHidden + ( nHidden + 1 ) * nClass
	nn <- nnet( trainSub, trainClassIndSub, size = nHidden, rang = 0.1, decay = 5e-4, maxit = maxIter, trace = FALSE, MaxNWts = nWts )
	returnval <- max.col( predict( nn, testSub ) )
}

# code run by client after main loop
client_postscript <- function()
{
  dummy <- 0  # prevents output "NULL"
}

#
# section: head logic
#

# code run by head before main loop
head_preamble <- function()
{
	testClassPredAll <<- NULL
    nCorrTestPredAll <<- NULL
	nResults <<- 0
	testClassTrue <<- as.numeric( as.factor( test[ , nTrainCols ] ) )   # form factor, then convert from factor values to factor indexes
    
    print( "class populations in training set:", quote = FALSE )
    for ( lev in 1 : nClass )
    {
        print( paste( "   ", levels( trainClassTrueFactor )[ lev ], "   ", sum( trainClassTrue == lev ), sep = "" ), quote = FALSE )
    }
    print( " ", quote = FALSE )
    flush.console()
}

# collect the predicted classifications from all workers
head_resulthandler <- function(result, ensembleID) {
	testClassPred <- result
	testClassPred <- as.numeric( testClassPred )    # convert from factor values to factor indexes
	nCorrTestPred <- sum( testClassTrue == testClassPred )
	pctCorrTestPred <- 100.0 * nCorrTestPred / nTestRows
	eca_log( paste( "neural net ", ensembleID, " : ", nCorrTestPred, "/", nTestRows, " correct", sprintf( " ( %5.2f%% )", pctCorrTestPred ), sep = "" ), timeStamp = FALSE )
    flush.console()
	testClassPredAll <<- c( testClassPredAll, testClassPred )
    nCorrTestPredAll <<- c( nCorrTestPredAll, nCorrTestPred )
	nResults <<- nResults + 1
}

# code run by head after main loop
head_postscript <- function()
{
	# compute accuracy of predicted classifications
	dim( testClassPredAll ) <- c( nTestRows, nResults )
	counts <- array( rep( 0, nTestRows * nClass ), c( nTestRows, nClass ) )
	for ( samp in 1 : nTestRows )
	{
		for ( cla in 1 : nClass )
		{
			counts[ samp, cla ] = sum( testClassPredAll[ samp, ] == cla )
		}
	}
    set.seed( 1 )                           # set random seed so max.col gives reproducible results
    testClassPredMax = max.col( counts )

    eca_log( " ", timeStamp = FALSE )
    meanNCorrTestPred <- mean( nCorrTestPredAll )
    stdNCorrTestPred <- sqrt( var( nCorrTestPredAll ) )
    pctMeanNCorrTestPred <- 100.0 * meanNCorrTestPred / nTestRows 
    pctStdNCorrTestPred <- 100.0 * stdNCorrTestPred / nTestRows
    eca_log( paste( "individual neural network average : ", sprintf( "%8.2f +/- %8.2f / %d ( %5.2f +/- %5.2f %% )", meanNCorrTestPred, stdNCorrTestPred, nTestRows, pctMeanNCorrTestPred, pctStdNCorrTestPred ), sep = "" ), timeStamp = FALSE )
	nCorrEnsemPred <- sum( testClassTrue == testClassPredMax )
	pctCorrEnsemPred <- 100.0 * nCorrEnsemPred / nTestRows
    eca_log( paste( "ensemble of neural networks       : ", nCorrEnsemPred, " / ", nTestRows, " correct", sprintf( " ( %5.2f%% )", pctCorrEnsemPred ), sep="" ), timeStamp = FALSE )
    eca_log( " ", timeStamp = FALSE )
}
