# R helper functions for use with RMPI and MapReduce framework implementations 
# of Insilicos Ensemble Cloud Army

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

eca_verbose <- function() {         # are we being chatty?
    result = ("True" == eca_config$verbose)
}

eca_runLocal <- function() {
    result <- ("True" == eca_config$runLocal)
}

eca_log <- function( textA, textB = "", textC = "", timeStamp = TRUE ) {
    logstr <- paste( textA, textB, textC )
    if ( timeStamp ) {
        logstr <- paste( date(), logstr )
    }
    if ( eca_verbose() ) {
		if ( runningAsClient ) {
			glob_logstrs <<- c( glob_logstrs, paste( "(client ", mpi.comm.rank( 1 ), ") ", logstr, sep="" ) )
		}
		if (runningAsHadoopMapper) {
			glob_logstr <<- paste(glob_logstr,logstr) # accumulate log for final transmission to reducer
		}
    }
	if (runningAsHadoopMapper) {
		cat( logstr, "\n", file = stderr() )
	} else {
		cat( logstr, "\n" )
	}
}

eca_debug <- function( textA, textB = "", textC = "", timeStamp = TRUE ) {
   if ( eca_verbose() ) {
        eca_log(textA, textB, textC, timeStamp)
	}
}

eca_dump_log <- function() {
	saved_logstrs <- glob_logstrs
	glob_logstrs <<- c("") # reset
	retval <- saved_logstrs
}

# inspect named file to determine format, and load into memory, with optional limit on row count
# currently only format is kind handled by "read.table"
# the idea is to expand later to other file formats
# legal values of hasHdr passed in: 0 means file known to not have header, 1 means file known to have header
eca_read_datafile <- function(filename,rowlimit=-1,hasHdr=-1) {
	connection <- eca_readpipe(filename,2) # read just two lines for inspections
	# read first two lines
	lines <- readLines(connection) 
	close(connection)
	# count number of tabs, space, commas - assume most abundant is the seperator
	commas <- length(gregexpr('[,]',lines[1])[[1]])
	spaces <- length(gregexpr('[ ]',lines[1])[[1]])
	if (commas > spaces) {
	   seper <-  ','
	   best <- commas
	} else {
		seper <- ' '
		best <- spaces
	}
	tabs <- length(gregexpr('[\t]',lines[1])[[1]])
	if (tabs > best) {
	   seper <- '\t'
	}

    if ( hasHdr != 0 & hasHdr != 1 ) {
		# no guidance on header provided in call to function; need to detect
		# expect header to have fewer numerical entries
		regexstr='(?<!([a-zA-Z]))([-+]?([0-9]*[.])?[0-9]+([eE][-+]?[0-9]+)?)'
		numbercount1 <- length(gregexpr(regexstr,lines[1],perl=TRUE)[[1]])
		numbercount2 <- length(gregexpr(regexstr,lines[2],perl=TRUE)[[1]])
		hasHdr <- (numbercount1 < numbercount2)
    }

	# OK, now actually read
	connection2 <- eca_readpipe(filename,rowlimit)
    read.table(connection2, header = hasHdr, sep = seper) # this is the return value
}

# function to handle possibly gzipped input files
eca_readpipe <- function(fname, nlines=-1) {
   if ( ! file.exists( fname ) ) {
      # probably because it's actually in the data dir after being copied off S3
      fname = paste(eca_config$dataDir,fname,sep="")
   }
   pipe(eca_construct_pipe_cmd(fname,nlines))
}

# construct a commandline to pipe a file (possibly HDFS, gzip, linecount limited)
eca_construct_pipe_cmd <- function(fname, nlines=-1) {
	limit_cmd=""
	gz_cmd = ""
	gzipped = length(i <- grep(".gz$",fname))
	if (length(i <- grep("hdfs://",fname))) { # HDFS file?
		cat_cmd = "hadoop dfs -cat "
		if (gzipped) {
			gz_cmd = " | gzip -dc"
		}
		if (nlines > 0) {
			limit_cmd = paste(" | head -",nlines,sep="")
		}
	} else { # not HDFS
		if (gzipped) {
			cat_cmd = "gzip -dc "
			if (nlines>0) {
				# capture gzip's stderr in case gzip complains of broken pipe when head shuts down after 2 lines
				limit_cmd = paste(" 2>&1 | head -",nlines,sep="")		}
		} else {
			if (nlines>0) {
				cat_cmd = paste("head -",nlines," ",sep="")
			} else {
				cat_cmd = "cat "
			}
		}
	}
	# now construct shell command to list maybe nlines of maybe gzipped file maybe on HDFS
	paste(cat_cmd,fname,gz_cmd,limit_cmd,sep="")
}

#
# range-checked string to int conversion, understands "all" (=maxval)
#
eca_asPositiveInt <- function( valueString, maxValue) {
	if (valueString == "all")
		result = maxValue
	else {
		result = as.numeric(valueString)
		if (result < 0) {
			eca_log("error: negative value passed to eca_asPositiveInt: ",valueString)
			quit()
		} else if (result > maxValue) {
			result = maxValue;
		}
	}
	returnval <- result
}

.Last <- function() {
    if ( is.loaded( "mpi_initialize" ) ) {
        if ( mpi.comm.size( 1 ) > 0 ) {
            eca_log( "Please use mpi.close.Rslaves() to close workers." )
            mpi.close.Rslaves()
        }
        eca_log( "Please use mpi.quit() to quit R" )
        .Call( "mpi_finalize" )
    }
}
