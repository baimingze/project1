# R functions to run a decision forest (ensemble of decision trees)
#  with k-fold cross-validation, as directed by the RMPI framework
#  for Insilicos Ensemble Cloud Army. For use in situations where there
#  is no pre-defined separation of data into training and test sets.

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
    library( "rpart" ) 
    # load dataset
    # TODO: more general input handling (Weka ARFF or saved R dataframes)
    sharedFile_data <- eca_config$sharedFile_GZ_data

    # note use of eca_read_datafile() to allow gzip handling, format recognition
    dat <<- eca_read_datafile( sharedFile_data )
    # note dimensions ( <<- instead of <- makes them globals)
    nDataRow <<- dim( dat )[ 1 ]
    nDataCol <<- dim( dat )[ 2 ]
    # TODO: deal with case where test has class values not present in train
    nClass <<- length( unique( dat[ , nDataCol ] ) )
    nFold <<- as.numeric( eca_config$numberOfFolds )
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
    set.seed( 1 )
    randSeq <<- sample( nDataRow )    # random sequence of integers, for forming random folds
    dummy <- 0  # prevents output "NULL"
}

# accepts a unique ID integer (used here as a random seed), performs ensemble calc
client_ensemblecalc <- function( randSeedVal )
{
	# range-checked string to int conversion, understands "all" (=maxval)
    nInstSub <- eca_asPositiveInt( eca_config$numberInstancesInSubset, nDataRow )
    # print( paste( "tree set ", randSeedVal, sep = "" ), quote = FALSE )   # can uncomment for local mode
    set.seed( randSeedVal )
    testClassPred <- rep( 0, nDataRow )

    for ( fold in 1 : nFold )
    {
        # print( paste( "   training tree on fold ", fold, sep = "" ), quote = FALSE )   # can uncomment for local mode
        # flush.console()
        
        # create fold
        startRandSeq <- floor( ( fold - 1 ) * nDataRow / nFold ) + 1
        endRandSeq <- floor( fold * nDataRow / nFold )
        foldIdx <- randSeq[ startRandSeq : endRandSeq ]
        train <- dat[ -foldIdx, ]
        test <- dat[ foldIdx, ]
        nTrainInst <- dim( train )[ 1 ]
        nTestInst <- dim( test )[ 1 ]

        # create training set for individual classifier from full training set by random sampling without replacement
        trainSubIdx <- sample( nTrainInst, nInstSub )
        trainSub <- train[ trainSubIdx, ]

        form <- as.formula( paste( names( train )[ nDataCol ], ".", sep = "~", collapse = "" ) )
        # TODO: make control parameters settable via config file
        fit <- rpart( form, data = trainSub, method = "class", control = rpart.control( minsplit = 25, cp = 0.001, maxdepth = 15 ) )
        pred <- predict( fit, newdata = test, type = "class" ) # return value is testClassPred, a "factor of classifications" per http://cran.r-project.org/web/packages/rpart/rpart.pdf
        testClassPred[ foldIdx ] <- pred
    }
    returnval <- testClassPred
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
    classTrueFactor <- as.factor( dat[ , nDataCol ] )   # form factor, then convert from factor values to factor indexes
	classTrue <<- as.numeric( classTrueFactor )

    print( "class populations in dataset:", quote = FALSE )
    for ( lev in 1 : nClass )
    {
        print( paste( "   ", levels( classTrueFactor )[ lev ], "   ", sum( classTrue == lev ), sep = "" ), quote = FALSE )
    }
    print( " ", quote = FALSE )
    print( paste( "Each tree set consists of ", nFold, " trees, one tree for each of ", nFold, " folds of data.", sep = "" ), quote = FALSE )
    flush.console()
}

# collect the predicted classifications from all workers
head_resulthandler <- function(result, ensembleID) {
	testClassPred <- result
	testClassPred <- as.numeric( testClassPred )    # convert from factor values to factor indexes
	nCorrTestPred <- sum( classTrue == testClassPred )
	pctCorrTestPred <- 100.0 * nCorrTestPred / nDataRow
	eca_log( paste( "   tree set ", ensembleID, " : ", nCorrTestPred, "/", nDataRow, " correct", sprintf( " ( %5.2f%% )", pctCorrTestPred ), sep = "" ), timeStamp = FALSE )
	testClassPredAll <<- c( testClassPredAll, testClassPred )
    nCorrTestPredAll <<- c( nCorrTestPredAll, nCorrTestPred )
	nResults <<- nResults + 1
}

# code run by head after main loop
head_postscript <- function()
{
	# compute accuracy of predicted classifications
	dim( testClassPredAll ) <- c( nDataRow, nResults )
	counts <- array( rep( 0, nDataRow * nClass ), c( nDataRow, nClass ) )
	for ( samp in 1 : nDataRow )
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
    pctMeanNCorrTestPred <- 100.0 * meanNCorrTestPred / nDataRow
    pctStdNCorrTestPred <- 100.0 * stdNCorrTestPred / nDataRow
    eca_log( paste( "average of all tree sets : ", sprintf( "%8.2f +/- %8.2f / %d correct ( %5.2f +/- %5.2f %% )", meanNCorrTestPred, stdNCorrTestPred, nDataRow, pctMeanNCorrTestPred, pctStdNCorrTestPred ), sep = "" ), timeStamp = FALSE )
	nCorrEnsemPred <- sum( classTrue == testClassPredMax )
	pctCorrEnsemPred <- 100.0 * nCorrEnsemPred / nDataRow
    eca_log( paste( "ensemble of all tree sets: ", nCorrEnsemPred, " / ", nDataRow, " correct", sprintf( " ( %5.2f%% )", pctCorrEnsemPred ), sep="" ), timeStamp = FALSE )
    eca_log( " ", timeStamp = FALSE )
}
