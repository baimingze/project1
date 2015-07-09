# R functions to run a machine learning ensemble, as directed by the RMPI framework
#  for Insilicos Ensemble Cloud Army

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

########################################
# USAGE NOTE: this is a trivial example to illustrate the roles of the R functions that
# run a machine learning ensemble under the RMPI framework for Insilicos Ensemble Cloud
# Army.
########################################

# 
# section: code common to client and head modes
#

# runs just after config is loaded
# this is typically where you load data files etc
common_preamble <- function() {
	if (eca_runLocal()) { # this returns true when running locally for prototyping
		msg = "hello from the local prototyping task that runs both head and client code!" 
	} else {
		msg = "hello from the head node or a client!"
	}
	# eca_log() and eca_config are defined by the framework script
	eca_log(paste("in ",eca_config$scriptFileName,": ",msg,sep=""))
}
# last thing run
# leaving this empty to show what that looks like: it can't actually be completely empty (R complains)
common_postscript <- function() {
  # eca_log("goodbye!")
  dummy<-0  # prevents R's default behavior, which is to output "NULL"
}

#
# section: client logic 
#

# code run by client before main loop
client_preamble <- function()
{
  eca_log("hello from a client node!")
}
# this is where the heavy lifting is normally done - typically the ensembleID (an integer) is used 
# as a random seed.  It is guaranteed to be unique across all invocations of this function
client_ensemblecalc <- function(ensembleID)
{
	paste("hello from client ensemble calc with ensembleID=",ensembleID,sep="")  # we'll just return a string
}

# code run by client after main loop
client_postscript <- function()
{
  eca_log("goodbye from a client node!")
}

#
# section: head logic
#
# code run by head before main loop
head_preamble <- function()
{
	eca_log("hello from head node!")
}
# function to aggregate each ensemble's results - in this 
# trivial example each ensemble just returns a string so we'll
# print them all, normally it's a more complicated task of 
# combining more complicated objects
head_resulthandler <- function(result, ensembleID) {
	eca_log(paste("head_resulthandler got \"",result, "\" from client with ensembleID ",ensembleID,sep=""))
}

# code run by head after main loop
head_postscript <- function() {
	eca_log("goodbye from head node")
} 

