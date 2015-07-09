#!/opt/local/bin/python

# helper functions for launching ensemble learning jobs in Amazon EC2 using RMPI

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

import sys
import os
import platform
import subprocess
import simplejson as json
from gzip import GzipFile
import zlib
import base64
import tempfile
import fileinput
import re

import boto
import boto.ec2
import boto.s3
from boto.ec2.regioninfo import RegionInfo
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import datetime
import string

from optparse import OptionParser

# current StarCluster AMIs
CURRENT_RELEASE_AMI_32="ami-8cf913e5"  # starcluster-base-ubuntu-10.04-x86-rc3
CURRENT_RELEASE_AMI_64="ami-0af31963"  # starcluster-base-ubuntu-10.04-x86_64-rc1
CURRENT_RELEASE_AMI_CCI="ami-12b6477b" # starcluster-base-centos-5.4-x86_64-ebs-hvm-gpu-rc2

cfgStack=[] # a stack of configs, for use in a batch scenario
currentCfgID=-1 # stack index for current config, or core cfg if -1
cfgCore={"eca_launch_helper_version":"0.2"} # config parameters common to all configs in stack

next_logmsg_needs_leading_newline = False # for pretty logs
logStartTime = datetime.datetime.utcnow()


# add another config for batch scenario
def pushConfig() :
	cfgCopy ={}
	cfgStack.extend([cfgCopy])

# use a given copy of config
def selectConfig(n=None) :
	global currentCfgID
	if None != n : # just getting handle on the current selection?
		currentCfgID = n
	if (-1 == currentCfgID) :
		return cfgCore
	else :
		return cfgStack[currentCfgID]

# read a json file possibly with python/R style comments (#<everything to EOL>)
# also tolerates a json string instead of a filename reference
# also accepts a list of files that contain json
# returns the number of configs found
def load_commented_json(filename,multiconfigOK) :
	configStr = ""
	if (filename.startswith('{')) : # form {...}
		configStr = filename  # not actually a filename, just a json string
		load_json_string(configStr)
		return 1
	elif (filename.startswith('[')) : # form [{...},{...}]
		configStr = filename  # not actually a filename, just a nested json string
		jsonstrings = []
		while len(configStr) :
			parts = configStr.partition("},{")
			configStr = parts[0]
			if configStr.startswith('[') :
				configStr = configStr[1:]
			if configStr.endswith(']') :
				configStr = configStr[:len(configStr)-1]
			if not configStr.startswith("{") :
				configStr = "{"+configStr
			if not configStr.endswith("}") :  
				configStr += "}"
			jsonstrings.extend([configStr])
			configStr = parts[2]
		nConfigs = len(jsonstrings)
		for n in range(nConfigs) :
			pushConfig() # add a new blank config onto the stack
		for n in range(nConfigs) :
			selectConfig(n)
			load_commented_json(jsonstrings[n],False)            
		return nConfigs
	else :
		log("processing parameter file %s"%filename)
		f = open(filename,"r")
		strings = f.readlines()
		nConfigs = 0
		names = []
		badnames = []
		if True == multiconfigOK : # check to see if this is a file of filenames
			for ss in strings :
				s = ss.strip()
				if s.startswith('[') and (1 == len(strings)) : # actually a json array
					return load_commented_json(s,True)
				if (len(s) and not s.startswith("#")) :
					if (os.path.exists(s)) :
						cfgCore["coreBaseName"]=tidypath(filename)
						log("adding parameter file %s to the run"%s)
						names.extend([s])
						nConfigs = nConfigs+1
					else :
						badnames.extend([s])
		if (nConfigs > 1) :
			if len(badnames) :
				errmsg = "failed to open config file(s):"
				for badname in badnames :
					errmsg += " "
					errmsg += badname
				raise RuntimeError(errmsg)
				return 0
			for n in range(nConfigs) :
				pushConfig() # add a blank config onto the stack
			for n in range(nConfigs) :
				selectConfig(n)
				load_commented_json(names[n],False)
				setConfig("eca_cfgName",tidypath(names[n]))
			tidyConfigStack() # move any redundant info to the core config
			return nConfigs
		else :
			if True == multiconfigOK :
				# this was the call to determine if this file had a list of files, it didn't
				pushConfig()
				setConfig("eca_cfgName",tidypath(filename))
			for s in strings :
				configStr += (s.split('#')[0]).split('\n')[0]
			load_json_string(configStr)
			return 1

# read a json string, add to overall parameters
def load_json_string(config) :  
	params = json.loads(config)
	# now add (possibly override) these parameters to those already seen
	for key,val in params.iteritems():
		try:  # val may be an int
			# make boolean values case insensitive
			if ("true" == val.lower()) :
				val = "True"
			if ("false" == val.lower()) :
				val = "False"
		except:
			pass
		# save to global config parameters
		setConfig(key,val)
	return params

# consistent drive letter case
def drivecaps(name) :
	fname = name
	if (len(fname) > 2) and (":"==fname[1]) :
		fname = string.lower(fname[0])+fname[1:] # consistent case for drive letters
	return fname

# examine an R script for library() calls and produce another R script
# that makes sure those packages are loaded
def create_R_package_loader_script( rscriptFilename ) :
	result = "\
	eca_install_package <- function(libname) {\n\
		package <- as.character(substitute(libname))\n\
		max_retry = 5\n\
		for (retry in 0 : max_retry) {\n\
			if ( package %in% .packages(all.available=TRUE) ) {\n\
				print(paste('package',package,'is installed, good.'))\n\
				break;\n\
			} else if (retry == max_retry) {\n\
				print(paste('unable to install package',package))\n\
			} else {\n\
				# choose a mirror at random\n\
				m=getCRANmirrors(all = TRUE)\n\
				repository=m[sample(1:dim(m)[1],1),4]\n\
				if (retry) {\n\
					print('retry...')\n\
				}\n\
				print (paste('installing package',package,'from randomly chosen repository',repository))\n\
				install.packages(package,repos=repository)\n\
			}\n\
		}\n\
	}\n"
	for line in fileinput.FileInput(rscriptFilename):
		if (re.search("library[ \t]*\(",line)!=None) :
			line = line.replace("library","eca_install_package")
			result = result + line +"\n"
	result = result + " quit()\n"
	return result
		
# absolute path with tidied-up path seperators
def my_abspath(name) :
	fname = os.path.abspath( name )
	fname = drivecaps( fname )
	fname = tidypath( fname )
	return fname

# tidied-up path seperators
def tidypath( p ) :
	p = p.replace("\\","/")
	return p.replace("//","/")

# shall we be chatty?
def verbose() :
	isverbose = getDebug() or getConfig("verbose","False")
	setCoreConfig("verbose",isverbose,noPostSaveWarn=True) # make sure this has a value downstream
	if (getDebug()) : # verbose boto
		if (not boto.config.has_section('Boto')) :
			boto.config.add_section('Boto')
			boto.config.set('Boto', 'debug', '2')            
	return("True"==isverbose)

# crazy verbose?
def getDebug() :
	return "True" == getConfig("debug","False")

# are we running RMPI or mapreduce?
def runRMPI() :
	return ("RMPI" == getConfig("runStyle"))

# are we running in prototyping mode? 
def runLocal() :
	return ( getConfig( "runLocal" ) == "True" )

def runHadoop() :
	if (runLocal()) :
		return False
	elif runAWS() :
		return False
	# driving a bare Hadoop cluster, as opposed to using the AWS EMR API
	if ( "Windows" == platform.system() ) :
		log("This script uses a local install of hadoop for file transfers to the cluster.")
		log("Unfortunately hadoop on Windows only works under Cygwin.")
		log("Please try again from a Cygwin shell.  Quitting now.")
		exit(1)
	return True

# are we running on AWS (that is, not local, and not hadoop cluster)
def runAWS() :
	if (runLocal()) :
		return False
	if ( "True" == getConfig( "runAWS", "False" ) ) : #  force AWS and ignore hadoop setup?
		return True
	if ( None != getConfig("hadoop_dir",required=False) ) :
		return False  # no hadoop gateway mentioned
	return True

# write a timestamped message to stdout
def log(obj) :
	global next_logmsg_needs_leading_newline
	log_no_newline(obj)
	print "" # and a newline
	next_logmsg_needs_leading_newline = False

def log_no_newline(obj) :
	global next_logmsg_needs_leading_newline
	if (next_logmsg_needs_leading_newline) :
		print ""
	# deal with lists of strings, as well as simple strings
	text = ""
	if ( list == type(obj) ) :
		for o in obj:
			text = text + str(o) + " "
	else :
		text = str(obj)        
	print datetime.datetime.now().strftime("%a %b %d %H:%M:%S %Y")+" "+text,
	next_logmsg_needs_leading_newline = True

def log_no_timestamp( obj ) :
	print str( obj ),

def debug( obj) :
	if (getDebug()) :
		log(obj)

def info( obj) :
	if (verbose()) :
		log(obj)

# just put a dot
def log_progress() : 
	print '.', # no newline

# write that final newline
def log_close() :
	global logStartTime
	delta = datetime.datetime.utcnow() - logStartTime
	log( "elapsed time = "+str(delta))

# routine to retrieve config, with error checking
def loadConfig(runStyle):
	startTime = datetime.datetime.utcnow()

	if ( len( sys.argv ) < 3 ) :
		raise RuntimeError( "syntax: " + sys.argv[ 0 ] + " <cluster_config_file> <job_config_file> [ options ]" )

	parser = OptionParser()
	parser.add_option( "-l", "--local", action = "store_true", dest = "runLocal", default = False, help = "run ensemble on local computer" )
	( options, args ) = parser.parse_args()
	
	# do boto version check
	if (int(boto.Version.partition('.')[0]) < 2) :
		versionerr= "boto version 2.1 or higher is required (you have "+boto.Version+") - https://github.com/boto/boto/tarball/2.1rc2 is known to work"
		raise RuntimeError(versionerr)
	
	# read the cluster config file (encoded as json, possibly with comments)
	load_commented_json( args[ 0 ] , False) # false=don't accept a file that's a list of config files
	
	# we have trouble downstream if some config parameters are not set
	setCoreConfig( "runLocal", "True" if options.runLocal else "False" )

	# read the job config file(s) (encoded as json or a list of files containing json, possibly with comments)
	nJobs = load_commented_json( args[ 1 ] , True) # true=OK to accept a file containing a list of config files

	# and see if there's any commandline JSON stuff, like maybe '{"debugEMR":"True"}'
	if (len(sys.argv) >= 4) :
		for n in range(2,len(sys.argv)) :
			if sys.argv[n].startswith("{") :
				log( "process additional args %s" % sys.argv[n] )
				load_commented_json(sys.argv[n], False)

	tidyConfigStack() # move any redundant info to the core config

	info( "job started" )

	# use job config filename as basename if none given
	setCoreConfig("baseName",S3CompatibleString(getConfig("baseName",os.path.basename(sys.argv[2]))))
	# possibly we already have a timestamp, if we're being called for X!!Tandem purposes
	setCoreConfig("jobTimeStamp", getConfig("jobTimeStamp",startTime.strftime("%Y%m%d%H%M%S")))       

	# note RMPI vs MapReduce mode
	setCoreConfig("runStyle",runStyle)


	# default to 64 bit architecture
	instance_type = getConfig("ec2_instance_type","") # modern way
	if ("" != instance_type) :
		setCoreConfig("ec2_client_instance_type",instance_type);
		if ( runRMPI() ) :
			setCoreConfig("ec2_head_instance_type",instance_type);
		else : # don't need much power for hadoop head node
			setCoreConfig("ec2_head_instance_type","m1.small");
	else : # legacy, or EMR
		# set head type to client type if only client type was specified
		client_type = getConfig("ec2_client_instance_type",required=False)  # possibly None
		head_type = getConfig("ec2_head_instance_type",client_type,required=False) # possibly None
		setCoreConfig("ec2_head_instance_type",head_type)
		# else set head type to large if not specified
		head_type = getConfig("ec2_head_instance_type","m1.large")
		setCoreConfig("ec2_head_instance_type",head_type)
		client_type = getConfig("ec2_client_instance_type",getConfig("ec2_head_instance_type"))
		setCoreConfig("ec2_client_instance_type",client_type)

	if ( runRMPI() and not runLocal() ) :    
		# Determine image types for head and client.
		# Some legacy code here - we used to allow these to be different for MPI, now we treat
		# them as synonyms with client nodes specs winning in event of conflict.
		# They can still differ for EMR, though.
		#
		# Make sure a few things have reasonable defaults.
		setCoreConfig("ami_32bit_id",getConfig("ami_32bit_id",CURRENT_RELEASE_AMI_32))
		setCoreConfig("ami_64bit_id",getConfig("ami_64bit_id",CURRENT_RELEASE_AMI_64))
		setCoreConfig("ami_cci_id",getConfig("ami_cci_id",CURRENT_RELEASE_AMI_CCI))
		aws_access_key_id = getConfig("aws_access_key_id")
		aws_secret_access_key = getConfig("aws_secret_access_key")
		# ensure sane instance types for given architectures
		i386types = { "t1.micro":1, "m1.small":1, "c1.medium":2 }
		ccitypes = { "cc1.4xlarge":1, "cg1.4xlarge":1, "cc2.8xlarge":2 }
		if (getConfig("ec2_head_instance_type") in ccitypes) :
			setCoreConfig("ami_head_id",getConfig("ami_cci_id"))  # cluster compute instance
			setCoreConfig("is_cci","True")
		elif (not getConfig("ec2_head_instance_type") in i386types) :
			setCoreConfig("ami_head_id",getConfig("ami_64bit_id"))
		else :
			setCoreConfig("ami_head_id",getConfig("ami_32bit_id"))
		if (getConfig("ec2_client_instance_type","") in ccitypes) :
			setCoreConfig("ami_client_id",getConfig("ami_cci_id"))  # cluster compute instance
		elif (not getConfig("ec2_client_instance_type","") in i386types) :
			setCoreConfig("ami_client_id",getConfig("ami_64bit_id"))
		else :
			setCoreConfig("ami_client_id",getConfig("ami_32bit_id"))

		if ((getConfig("ami_head_id") != getConfig("ami_client_id")) or ( getConfig( "ec2_head_instance_type" ) != getConfig( "ec2_client_instance_type" ))) :
			log("Head and client nodes must use same AMI and instance type. Formerly we supported mixed AMIs but have dropped this for launch efficiency.  We will use client node specifications for all nodes.")
			setCoreConfig("ami_head_id",getConfig("ami_client_id"))
			setCoreConfig("ec2_head_instance_type", getConfig( "ec2_client_instance_type" ))

		if ( getConfig("ec2_endpoint","") != "" ) :
			# explict endpoint requires explicit region
			aws_region = boto.ec2.connection.RegionInfo(name=getConfig("aws_region"),endpoint=getConfig("ec2_endpoint"))
			conn = aws_region.connect(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
		else :
			# figure out what region this AMI is in
			for region in boto.ec2.regions(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key) :
				conn = region.connect(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
				try:
					if (None != conn.get_image(getConfig("ami_head_id"))) :
						setCoreConfig("aws_region",region.name)
						break
				except:
					continue
		headimage = conn.get_image(getConfig("ami_head_id"))
	else :  # mapreduce - default to small head node, large clients
		if (not runLocal() ) : 
			# specify region for EMR        
			setConfig("aws_region",getConfig("aws_region","us-east-1"))
			setConfig("ec2_head_instance_type", getConfig("ec2_head_instance_type","m1.small"))
			setConfig("ec2_client_instance_type",getConfig("ec2_client_instance_type","m1.large"))

	# default number of R tasks per client if none specified: 1 per client core
	corecounts = { "t1.micro" : 1, "m1.small" : 1, "m1.large" : 2, "m1.xlarge" : 4, \
				   "m2.large" : 2, "m2.2xlarge" : 4, "m2.4xlarge" : 8, \
				   "c1.medium" : 2, "c1.xlarge" : 8, "cc1.4xlarge" : 8, "cg1.4xlarge" : 8 }
	if ( ( "" == getConfig( "numberOfRTasksPerClient" , getConfig( "numberOfRTasksPerNode","" ) )) and not runLocal() ) :
		clientType = getConfig( "ec2_client_instance_type" )
		if ( clientType in corecounts ) :
			setCoreConfig( "numberOfRTasksPerClient", corecounts[ clientType ] )
		if ( "" == getConfig( "numberOfRTasksPerClient", "" ) ) :
			unknownTaskCount = "unable to determine proper number of R tasks per client for client type " + clientType
			raise RuntimeError( unknownTaskCount )

	if ( ( "" == getConfig( "numberOfRTasksOnHead" , "" ) ) and not runLocal() ) :
		headType = getConfig( "ec2_head_instance_type" )
		# OpenMPI seems to treat slots as meaning "slaves", so for the head node
		# slots=8 will get you 9 processes - Rank0 and Ranks 1-7 - so we ask for one less on head
		# at runtime if we find it's not OpenMPI we bump that up by one
		if ( "" != getConfig( "numberOfRTasksPerNode","" )) :
			setCoreConfig( "numberOfRTasksOnHead", int(getConfig( "numberOfRTasksPerNode")) - 1 ) # leave a slot for master task
		elif ( headType in corecounts ) :
			setCoreConfig( "numberOfRTasksOnHead", corecounts[ headType ] - 1 ) # leave a slot for master task
		if ( "" == getConfig( "numberOfRTasksOnHead", "" ) ) :
			unknownTaskCount = "unable to determine proper number of R tasks for head node type " + headType
			raise RuntimeError( unknownTaskCount )

	# sets a few useful globals
	coreJobDir=getCoreJobDir()
	# save results to local file?  (defeated by setting "resultsFilename":"")
	for n in range(0,len(cfgStack)) :
		selectConfig(n)
		if ("_none" == getConfig("resultsFilename","_none")): # no results name set yet
			setConfig("resultsFilename",my_abspath(coreJobDir+"/%s.results.txt"%getConfig("eca_uniqueName")))


def tidyConfigStack() :
	# remove any values redundant to core                    
	for cfgA in cfgStack :
		deadkeys = {} # list of keys to remove
		for key,val in cfgA.iteritems():
			if (key in cfgCore) and (cfgCore[key] == val) :
				deadkeys[key] = 1 # note redundant local declaration
		for key in deadkeys :
			if len(key):
				del cfgA[key]
	
	# move any key,value found identically in all cfgStack members to core
	deadkeys = {} # list of keys to remove
	for cfgA in cfgStack :
		for key,val in cfgA.iteritems():
			consistent = True
			for cfgB in cfgStack :
				if (not key in cfgB) or (cfgB[key] != val) :
					consistent = False
					break
			if (key in cfgCore) and (cfgCore[key] != val) :
				consistent = False
			if True == consistent :
				cfgCore[key] = val # move to core config
				deadkeys[key] = 1 # mark for removal from config stack
	for key in deadkeys :
		if len(key):
			for cfgB in cfgStack :
				del cfgB[key]

	# get a unique identifier for each config
	save = currentCfgID # push
	nConfigs = len(cfgStack)
	for n in range(nConfigs) :
		selectConfig(n)
		setConfig("eca_uniqueName",os.path.basename(getConfig("eca_cfgName")))
	all_unique = True
	for n in range(nConfigs) :
		selectConfig(n)
		matches = 0
		check = getConfig("eca_uniqueName")
		for nn in range(nConfigs) :
			selectConfig(nn)
			matches += (check == getConfig("eca_uniqueName"))
		all_unique &= (matches == 1)
	if (not all_unique) :
		for n in range(nConfigs) :
			setConfig("eca_uniqueName","cfg%d"%n)
	selectConfig(save) # pop


# retrieve config, with error checking
def getConfig( key, default = None, required = True, includeCoreCfg = True ) :
	if (currentCfgID >= 0) and ( key in cfgStack[currentCfgID] ) : # is key in current parameter set?
		val = cfgStack[currentCfgID][ key ]
		if ( None != val ) :
			return val
	if (includeCoreCfg or (-1 == currentCfgID)) and ( key in cfgCore ) : # is key in common parameter set shared by all batch members?
		val = cfgCore[ key ]
		if ( None != val ) :
			return val
	if ( None != default ) :
		return default
	if ( required ) :
		raise Exception( "no value given for %s" % key )
	return None

def getConfigToString(cfgNum=-1) : # write as { <core stuff>,[<array of configs in stack>]}
	tidyConfigStack() # move anything common to all batch configs to core config
	cfgString = json.dumps(cfgCore)
	cfgString = cfgString[:len(cfgString)-1] # drop the trailing }
	if (cfgNum >= 0) : # individual cfg
		cfgString += ","
		cfgString += json.dumps(cfgStack[cfgNum])
		cfgString += "}\n"
	else :
		cfgString += ",\"cfgStack\":["
		comma=""
		for cfg in cfgStack:
			cfgString += comma
			cfgString += json.dumps(cfg)
			comma = ","
		cfgString += "]}\n"
	return cfgString

def getConfigAsRList(cfgNum=-1) : # optional cfg selector, default is all configs at once
	tidyConfigStack() # move anything common to all batch configs to core config
	if (cfgNum >= 0) : # specific config
		configStr = "eca_config=list("
		comma=""
		for key,val in cfgCore.iteritems():
			configStr = configStr+comma+"\n\""+key+"\"=\""+str(val)+"\""
			comma=","
		for key,val in cfgStack[cfgNum].iteritems():
			configStr = configStr+comma+"\n\""+key+"\"=\""+str(val)+"\""
			comma=","
		configStr = configStr+"\n)\n"
	else :
		configStr = "eca_configs=list("
		outercomma = ""
		for cfg in cfgStack:
			configStr += '%s\"%s\"=list(' % (outercomma,cfg["eca_uniqueName"])
			comma=""
			outercomma = ","
			for key,val in cfgCore.iteritems():
				configStr = configStr+comma+"\n\""+key+"\"=\""+str(val)+"\""
				comma=","
			for key,val in cfg.iteritems():
				configStr = configStr+comma+"\n\""+key+"\"=\""+str(val)+"\""
				comma=","
			configStr = configStr+"\n)\n"
		configStr = configStr+"\n)\n"
	return configStr

# set a value in the currently active config
def setConfig( key, val, noPostSaveWarn=False ) :
	if (("" != getConfig("savedAs","")) and (noPostSaveWarn != True) ):
		log("warning: setting value for %s in an already-saved configuration"%key)
	if (-1==currentCfgID) :
		setCoreConfig(key,val)
	else:
		debug("set %d %s=%s"%(currentCfgID,key,val))
		cfgStack[currentCfgID][ key ] = val

# set a parameter common to all configs in stack
def setCoreConfig( key, val, noPostSaveWarn=False) :
	if (("" != getConfig("savedAs","")) and (noPostSaveWarn != True) ):
		log("warning: setting value for %s in an already-saved configuration"%key)
	debug("set core %s=%s"%(key,val))
	cfgCore[ key ] = val # set in common area
	for cfg in cfgStack :
		if key in cfg :
			del cfg[key]    # kill in all configs

def getJobDir() :
	jobDir = ""
	if len(cfgStack) > 1 : # multi config batch job
		jobDir = getCoreJobDir()+"/"+getConfig( "eca_uniqueName" )
	else :    
		baseName = getConfig( "baseName" ) 
		job_ts = getConfig("jobTimeStamp")
		jobDir = '%s_runs/%s' % ( baseName , job_ts )
		resultsDir = baseName
	if not os.path.exists(jobDir):
		os.makedirs(jobDir)
	return jobDir

def getCoreJobDir() :
	if len(cfgStack) > 1 : # multi config batch job
		baseName = getConfig( "coreBaseName" )
		job_ts = getConfig("jobTimeStamp")
		if ( runLocal() ) :
			coreJobDir = '/tmp/%s/%s' % ( notDrive(baseName) , job_ts )
		else :    
			coreJobDir = '%s_runs/%s' % ( baseName , job_ts )
	else :
		coreJobDir = getJobDir()

	if not os.path.exists(coreJobDir):
		os.makedirs(coreJobDir)        
	return coreJobDir

# remove any security-sensitive settings from config then save to S3
# note that filename extension determines output format: .json or .r
def scrubAndPreserveJobConfig(filename, cfgNum=-1) : # optional config selector, -1=all
	tidyConfigStack()
	saveConfig={"":""}
	for key,val in cfgCore.iteritems():
		saveConfig[key] = val
	# clean out security stuff!
	setCoreConfig("aws_access_key_id", "xxxx")
	setCoreConfig("aws_secret_access_key", "xxxx")
	setCoreConfig("RSAKey", "xxxx")
	setCoreConfig("RSAKeyName", "xxxx")
	if (filename.endswith(".r")) :
		configStr = getConfigAsRList(cfgNum)
	else :
		configStr = getConfigToString(cfgNum) + "\n" # trailing newline matters to some readers
		# escape quoted commas
		commaquote="__COMMA__QUOTE__"
		configStr = configStr.replace(",\"",commaquote)
		configStr = configStr.replace(",",",\n")
		configStr = configStr.replace(commaquote,",\"")
	# restore - we need that security stuff to connect
	for key,val in saveConfig.iteritems():
		if (len(key)) :
			setCoreConfig(key, val)
	saveStringToFile(configStr,filename)
	setCoreConfig("savedAs",filename);

# save the given string to the named S3 file, or locally if runLocal()
def saveStringToFile(str,filename) :
	full_filename = filename
	if ( runAWS() ) :
		full_filename = getConfig("s3bucketID")+"/"+S3CompatibleString(filename)
	try:
		if ( runLocal() ) :
			f = open(filename,"w")
			f.write(str)
			f.close()   
		else :
			if ( runAWS() ) :
				s3conn = S3Connection(aws_access_key_id=getConfig("aws_access_key_id"), aws_secret_access_key=getConfig("aws_secret_access_key"))
				s3Bucket = s3conn.create_bucket(getConfig("s3bucketID"))
				k = Key(s3Bucket)
			else :  # non-AWS hadoop
				k = HadoopConnection()
			k.key = filename
			k.set_contents_from_string(str)
	except Exception, exception:
		log( exception )
		log("failed saving to " + full_filename)
		log("exiting with error")
		exit(-1)

def notDrive( instr ) :        
	outstr = drivecaps( instr ) # consistent windows drive letter capitalization
	outstr = outstr.replace(":","-") # TODO: deal with all illegal characters
	return outstr

# make string both S3 and URL compatible
def S3CompatibleString( instr , isBucketName=False) :
	if ( runLocal() ) :
		return instr # no need for S3 style correction
	outstr = notDrive( instr ) # convert any windows drive letter stuff to a dirname
	outstr = outstr.replace("\\","/")
	if (isBucketName) : # extra tight rules on the actual bucket
		outstr = outstr.lower()
		outstr = outstr.replace("_","-")
	if ((instr != outstr) and verbose()) :
		log("note: using name \""+outstr+"\" instead of \""+instr+"\" for S3 and URL compatibility")
	return outstr

# for hadoop use
def constructCacheFileReference( bucketName , jobDirS3 , configName) :
	if (runAWS()) :
		return 's3n://%s/%s/%s#%s' % ( bucketName , jobDirS3 , configName , configName )
	else :
		return '%s/%s/%s#%s' % ( bucketName , jobDirS3 , configName , configName )

# check list of files for existance on S3 and match with local, upload to S3 as needed
# list entries have form (filenameKey, targetDir, wantGZ)
# filenameKey allows us to get at filename as config["<filenameKey>"]
# targetDir is the S3 directory we want it to end up in (if blank, then use the directory named in config["<filenameKey>"])
# if wantGZ is true and config["<filenameKey>"] does not end in ".gz", a local gzipped copy is
# created and that's what we upload.
#
# side effect: config["<filenameKey>"] is changed to the S3 path 
def uploadToS3(s3FileList, makePublic=False, noRename=False) :

	if ( runAWS() ):
		s3BucketID = S3CompatibleString(getConfig("s3bucketID"),isBucketName=True) # enforce bucket naming rules

		# note: creating connection object only, no test of credentials or network here
		s3conn = S3Connection(aws_access_key_id=getConfig("aws_access_key_id"), aws_secret_access_key=getConfig("aws_secret_access_key"))

		debug( "using S3 bucket " + s3BucketID + " ... " )
		try:
			s3Bucket = s3conn.create_bucket(s3BucketID)
		except Exception, exception:
			log( exception )
			log( "error: unable to connect to S3 bucket " + s3BucketID + ", check credentials" )
			log( "exiting with error" )
			exit(-1)
		debug( "bucket ok!" )
		
		debug( "verifying data and script files in s3:" )
	else : # straight hadoop, actually
		s3BucketID = getHadoopDir()
		
	entryConfigNum = currentCfgID # so we can restore current config selection at end
	for filePairs in s3FileList:
		(fileNameKey, configNum, targetDir, wantGZ) = filePairs
		selectConfig(configNum)
		localPath = getConfig(fileNameKey,"",includeCoreCfg=False) # don't reprocess coreCfg entries
		if len(localPath) :
			if ("" != localPath) :
				if ( noRename ) :
					s3FileName = localPath # S3 equivalent
				else :
					s3FileName = S3CompatibleString(localPath) # S3 equivalent
				if ("" != targetDir) :
					s3FileName = targetDir+"/"+os.path.basename(s3FileName)
				try:
					if ( wantGZ and ( not localPath.endswith(".gz") )) :
						# use .gz equivalent, make one if needed
						oldPath = localPath
						localPath = localPath+".gz"
						oldS3Path = s3FileName
						s3FileName = s3FileName+".gz"
						if (not os.path.exists(localPath)) :
							log( "creating gzipped copy of "+oldPath+" as "+localPath+" for transfer to S3" )
							try:
								file = open(oldPath,"rb")
								zipper = GzipFile(localPath,"wb")
								for line in file :
									zipper.write(line)
								zipper.close()
								file.close()
								setConfig(fileNameKey, localPath)
							except Exception, exception:
								log( exception )
								log( "Failed to create gzipped copy, using original.  This will work but is less efficient" )
								localPath = oldPath
								s3FileName = oldS3Path
						else :
							log( "based on filename, "+localPath+" appears to be gzipped copy of "+oldPath+" so we'll use that" )
					# now set config
					fp=open(localPath,"rb")
				except Exception, exception:
					log( exception )
					log( "error: could not open local file "+ localPath + " for S3 upload comparison! " )
					log( "exiting with error" )
					exit(-1)
				info( "checking for file " + s3FileName + " in S3 bucket ... " )
				if (runAWS()) :
					testKey = s3Bucket.get_key(s3FileName)
				else : # hadoop
					try :
						hadoopName = s3FileName
						if (not hadoopName.startswith(getHadoopDir())) :
							if (not hadoopName.startswith("/")) :
								hadoopName = "/"+hadoopName
							hadoopName = getHadoopDir()+hadoopName
						cmd = getHadoopBinary() + " dfs -ls "+hadoopName
						debug(cmd)
						lines = os.popen(cmd).readlines()
						testKey = None
						for t in lines :
							if ( t.find(hadoopName) > -1 ) :
								items = t.split()
								testKey = Key()
								testKey.size = int(items[4])
								
						result = 0
					except Exception, e :
						log( e )
						testKey = None
						result = 0
						
				if testKey == None:

					# file was not on S3,
					# upload if possible, or exit with error otherwise
					
					info( "uploading " + localPath + " to " + s3BucketID + "/" + s3FileName )
					# boto verifies file upload itself
					fp.close()
					if (runAWS()) :
						k = Key(s3Bucket)
					else :
						k = HadoopConnection()
					k.key = s3FileName
					k.set_contents_from_filename(localPath)
					if ( makePublic ) :
						k.set_acl('public-read')
				else:
					# file is on S3,
					# if we have a local copy, compare against it
						
					debug( "ok -- file found on S3" )

					# we have a local file to compare against,
					# continue with verifying file

					s3FileSize = testKey.size

					localFileSize = os.path.getsize(localPath)

					if (localFileSize != s3FileSize):
						log( "error: local and S3 file sizes of "+localPath+" do not match. Exiting without overwriting file" )
						log( localPath+" "+ str(localFileSize)+" vs S3 copy at "+str(s3FileSize) )
						log( "exiting with error" )
						exit(-1)
						
					if (runAWS()) :
						# get the hex digest version of the MD5 hash-- called the "etag" in aws parlance
						# (this comes back as a string surrounded by double-quote characters)
						s3HexMD5 = testKey.etag.replace('\"','')
					
						debug( "calculating local md5 hash..." )
						(localHexMD5,localB64MD5) = testKey.compute_md5(fp)
						debug( "done" )

						if (s3HexMD5 == localHexMD5):
							debug( "existing S3 copy of file "+localPath+" verified with correct size and checksum, good" )
							if ( makePublic ) :
								testKey.set_acl('public-read')
						else:
							log( "error: md5 sums for local and S3 copies of "+localPath+" did not match! Exiting without overwriting file" )
							log( "exiting with error" )
							exit(-1)
					fp.close()
					
				if not s3FileName.startswith("/") :
					s3FileName = "/"+s3FileName
				setConfig(fileNameKey, s3FileName) # now speak in terms of S3 for node config
	selectConfig(entryConfigNum) # restore entry state
	debug( "S3 tests complete" )

def downloadFromS3( filename ) :
	s3BucketID = S3CompatibleString(getConfig("s3bucketID")) # enforce bucket naming rules
	# note: creating connection object only, no test of credentials or network here
	s3conn = S3Connection(aws_access_key_id=getConfig("aws_access_key_id"), aws_secret_access_key=getConfig("aws_secret_access_key"))
	try:
		s3Bucket = s3conn.create_bucket(s3BucketID)
		k = Key(s3Bucket)
		k.key = filename
		return k.get_contents_as_string()
	except Exception, exception:
		log(exception)
		log( "error: unable to connect to S3 bucket " + s3BucketID + ", check credentials" )
		log( "exiting with error" )
		exit(-1)
		
def downloadFileFromS3( s3filename, localfilename ) :
	s3BucketID = S3CompatibleString(getConfig("s3bucketID")) # enforce bucket naming rules
	# note: creating connection object only, no test of credentials or network here
	s3conn = S3Connection(aws_access_key_id=getConfig("aws_access_key_id"), aws_secret_access_key=getConfig("aws_secret_access_key"))
	try:
		s3Bucket = s3conn.create_bucket(s3BucketID)
		k = Key(s3Bucket)
		k.key = s3filename
		return k.get_contents_to_filename(localfilename)
	except Exception, exception:
		log(exception)
		log( "error: unable to connect to S3 bucket " + s3BucketID + ", check credentials" )
		log( "exiting with error" )
		exit(-1)

def downloadFromS3IfExists( filename ) :
	s3BucketID = S3CompatibleString(getConfig("s3bucketID")) # enforce bucket naming rules
	# note: creating connection object only, no test of credentials or network here
	s3conn = S3Connection(aws_access_key_id=getConfig("aws_access_key_id"), aws_secret_access_key=getConfig("aws_secret_access_key"))
	try:
		s3Bucket = s3conn.create_bucket(s3BucketID)
		k = Key(s3Bucket)
		k.key = filename
		return k.get_contents_as_string()
	except Exception, exception:        # no handler, but need the fallthrough to no operation
		return


def calculateSpotBidAsPercentage( spotBid, instance_type, emrTax = 0.0 ) :
	demandprices = { "t1.micro" : 0.02, "m1.small" : 0.085, "m1.large" : 0.34, "m1.xlarge" : 0.68, \
			   "m2.large" : 0.50, "m2.2xlarge" : 1.00, "m2.4xlarge" : 2.00, \
			   "c1.medium" : 0.17, "c1.xlarge" : 0.68, "cc1.4xlarge" : 1.30, "cg1.4xlarge" : 2.10, \
			   "cc2.8xlarge" : 2.40}

	if ('%' in spotBid) : # a percentage, eg "25%" or "25%%"
		spotBid = "%.3f"%((1.0+emrTax)*demandprices[instance_type]*float(spotBid.rstrip("%"))*.01)
		
	return spotBid

# for use with EMR or straight hadoop
def getHadoopDir() :
	if (runAWS()) :
		return "/home/hadoop"
	else :
		return getConfig("hadoop_dir")


def explain_hadoop() :
	# tell user to init the socks proxy
	log("Hadoop cluster access doesn't seem to be set up!")
	log("if you haven't already initiated a proxy to your hadoop gateway, open another shell and leave the following command running in it:")
	log("\"ssh -D %s -n -N %s@%s\""%(getConfig("hadoop_proxy_port","6789"),getConfig("hadoop_user","<your_hadoop_username>"),getConfig("hadoop_gateway","<your_hadoop_gateway>")))
	log("see https://univsupport.hipods.ihost.com/documents/7/ for details on hadoop gateway proxies")
	log("and you'll need to install hadoop on your local machine, to get the hadoop file transfer commands working.")
	log("see http://hadoop.apache.org/common/docs/r0.15.2/quickstart.html and http://pages.cs.brandeis.edu/~cs147a/lab/hadoop-windows/")

def getHadoopHome() :	
	if "" == getConfig("hadoop_home","") :
		# not in our config file - set at system level?
		if "HADOOP_HOME" in os.environ :
			setConfig("hadoop_home",os.environ["HADOOP_HOME"])

	# set from config file if we bothered to set it
	os.environ["HADOOP_HOME"] = getConfig("hadoop_home")
		
	return os.environ["HADOOP_HOME"]
	
def getHadoopBinary() :
	return getHadoopHome()+"/bin/hadoop"

def cygwin() :
	return platform.system().startswith("CYGWIN")

# strip a filename of its cygwin weirdness
def decygwinify(filename) :
	if ( filename.startswith("/cygdrive/") ) : 
		return filename[10]+":"+filename[11:]  # /cygdrive/x/foo/bar -> x:/foo/bar
	elif (cygwin()) :
		filename = os.popen("cygpath -m "+filename).readline().rstrip("\r\n") # drop trailing newline
	return filename

# add cygwin weirdness to a filename if needed
def cygwinify(filename) :
	if (cygwin()) :
		if (':' == filename[1]) : # form x:/foo
			filename = os.popen("cygpath -u "+filename).readline().rstrip("\r\n") # drop trailing newline
		elif (not os.path.exists(filename) and not filename.startswith("/cygdrive/") ): # prepend with /cygdrive/x/
			filename = os.popen("pwd").readline().rstrip("\r\n")[:11]+"/"+filename 
	return filename

# for hadoop clusters, may need to set executable bit on script
def makeFileExecutable(remoteFilename) :
	if (runHadoop()) :
		ret = -7777 # magic number used below 
		try:
			args = [getHadoopBinary(),"dfs","-chmod","777",remoteFilename]
			debug( args )
			p = subprocess.Popen(args)
			result = p.communicate()[1]
			ret = p.wait()
			if (ret != 0) :
				log(result)
				log("problem with hadoop dfs -chmod (have you configured core-site.xml and mapred-site.xml?)")
				explain_hadoop()
				exit(ret)
		except Exception, exception:
			if (-7777 == ret) : # process never got started
				log("unable to invoke local copy of hadoop")
			log( exception )
			explain_hadoop()
			exit(1)

# useful for jamming stuff into EC2 userdata (16KB limit)
def string_as_base64_zlib( str ) :
	# remove any non-shebang comments
	stripcomments = re.compile('^[ ]*#[^!].+$',re.MULTILINE)
	str = stripcomments.sub('',str)
	z = zlib.compress(str,9)
	b64 = base64.b64encode(z)
	return b64

def file_contents_as_base64_zlib( fname ) :
	f = open(fname,"r")
	str = f.read()
	f.close()
	return string_as_base64_zlib( str )

# an in-memory tarball builder
# files is a list of pairs [filename,string]
# if string is nonempty, use that as file content
# otherwise read from local disk
def files_as_tar_gz_base64_string( files ) :
	# kudos to http://www.daniweb.com/software-development/python/code/254626
	# and http://stackoverflow.com/questions/740820/python-write-string-directly-to-tarfile
	import tarfile, stat, StringIO, zlib, time
	file_out = StringIO.StringIO()
	tar = tarfile.open(mode = "w:gz", fileobj = file_out)
	for f in files :
		content = StringIO.StringIO()
		slash = f[0].rfind('/')
		targetname = f[0][slash+1:]
		if ("" == f[1] ) : # static file
			h = open(f[0])
			content.write(h.read())
			h.close()
		else : # dynamic content
			content.write(f[1])
		content.seek(0)
		info = tarfile.TarInfo(name=targetname)
		info.size=len(content.buf)
		info.mode=stat.S_IEXEC|stat.S_IREAD|stat.S_IWRITE
		tar.addfile(tarinfo=info, fileobj=content)
	tar.close()
	return base64.b64encode(file_out.getvalue())

# mimic boto classes for use with non-AWS.EMR Hadoop
class HadoopConnection():
	def __init__(self, bucket=None, name=None):
		self.bucket = bucket
		self.name = name
		self.key = None  # used in set_contents_from_*

	class jobflowstatus() :
		def __init__(self,state=None):
			self.state = state
		
		
	def set_contents_from_filename(self, local_filename) :
		target_filename = self.key
		ret = -7777 # magic number used below 
		try:
			local_filename = decygwinify(local_filename) # for windows, remove cygwin hoo-hah if any
			if (not target_filename.startswith(getHadoopDir())) :
				target_filename = getHadoopDir()+"/"+target_filename
			args = [getHadoopBinary(),"fs","-put",local_filename,target_filename]
			debug( args )
			p = subprocess.Popen(args)
			result = p.communicate()[1]
			ret = p.wait()
			if (ret != 0) :
				log(result)
				log("problem with hadoop fs -put (have you configured core-site.xml and mapred-site.xml?)")
				explain_hadoop()
				exit(ret)
		except Exception, exception:
			if (-7777 == ret) : # process never got started
				log("unable to invoke local copy of hadoop for file transfer purposes")
			log( exception )
			explain_hadoop()
			exit(1)
		
	def set_contents_from_string(self, str) :
		f = tempfile.NamedTemporaryFile(delete = False)
		f.write(str)
		f.close()
		self.set_contents_from_filename(f.name)
		os.unlink(f.name)

	def get_contents_as_string(self) :
		args = [getHadoopBinary(),"fs","-cat",self.key]
		debug( args )
		p = subprocess.Popen(args)
		result = p.communicate()[1]
		ret = p.wait()
		if (None == result):
			result = ""
		return str(result)

	def set_acl(self, acl) :
		log( "HadoopConnection set_acl not implemented" )

	def run_jobflow(self, name, log_uri, ec2_keyname=None, availability_zone=None,
					master_instance_type=None,
					slave_instance_type=None,
					num_instances=None,
					action_on_failure='TERMINATE_JOB_FLOW',
					keep_alive=False,
					enable_debugging=False,
					hadoop_version=None,
					steps=[],
					bootstrap_actions=[]):
		# in EMR this fires off a task on the AWS management node.  In Hadoop we
		# are the managment node, so this actually just sets things up
		# and repeated calls to describe_jobflow() actually do the work
		self.worksteps = steps 
		self.current_stepnum = 0
		self.action_on_failure = action_on_failure
		

		return 0  # we don't actually support multiple job flows

	def add_jobflow_steps(self, steps=[]) :
		self.worksteps.extend(steps)

	def describe_jobflow(self, jf) : # this actually causes execution of the next workstep
		if (self.current_stepnum < len(self.worksteps)) :
			hadoopPath = getHadoopHome()
			hadoopVer = hadoopPath[hadoopPath.rfind("/")+1:]
			hadoopStreamingJar = decygwinify(hadoopPath+"/contrib/streaming/"+hadoopVer+"-streaming.jar")
			args = [getHadoopBinary(),"jar",hadoopStreamingJar]
			args.extend(self.worksteps[self.current_stepnum].args())
			debug(args)
			p = subprocess.Popen(args)
			result = p.communicate()[1]
			ret = p.wait()  
			if (ret != 0) :
				result = 'FAILED'
				if (self.action_on_failure=='TERMINATE_JOB_FLOW') :
					self.current_stepnum = len(self.worksteps) # so we don't try any more steps
			else :
				result = 'RUNNING'
			self.current_stepnum = self.current_stepnum+1
		else :
			result = 'COMPLETED'
		jfstatus = self.jobflowstatus()
		jfstatus.state = result
		return jfstatus

