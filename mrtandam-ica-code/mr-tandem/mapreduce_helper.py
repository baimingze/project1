#
# helper functions for launching X!Tandem jobs in Amazon EC2 using mapreduce
# also can be used to launch X!!Tandem (MPI-parallelized) jobs in AWS EC2 by
# leveraging the Ensemble Cloud Army work at http://sourceforge.net/projects/ica/
#
# Part of the Insilicos Cloud Army Project:
# see http://sourceforge.net/projects/ica/trunk for latest and greatest
#
# Copyright (C) 2010 Insilicos LLC  All Rights Reserved
# Based on code from Insilcos Ensemble Cloud Army:
# Original Authors Jeff Howbert, Natalie Tasman, Brian Pratt
#
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

import sys
import os
import re
import subprocess
import commands
import shlex
import tempfile
import platform
import simplejson as json
from gzip import GzipFile
import datetime
import string
from time import sleep

import boto
import boto.ec2
import boto.emr
import boto.s3
from boto.ec2.regioninfo import RegionInfo
from boto.s3.connection import S3Connection
from boto.s3.key import Key

cfgCore={"mrh_launch_helper_version":"0.2"}
cfgStack=[]
currentCfgID=-1  # cfgstack member -1 is the core config
logStartTime = datetime.datetime.utcnow()

# read a json file possibly with python/R style comments (#<everything to EOL>)
def load_commented_json(filename) :
    f = open(filename,"r")
    strings = f.readlines()
    config = ""
    for s in strings :
        config += (s.split('#')[0]).split('\n')[0]
    return load_json_string(config)

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

# note config settings from the core config file

# add to the config stack for use in multi-search scenario
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

# use the core config as the current set/get target
def selectCoreConfig() :
    return selectConfig(-1)

# strip the config stack of info that can be found in the core config,
# and move any config info common to all stack members to core
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

# tidied-up path seperators
def tidypath( p ) :
    p = p.replace("\\","/")
    return p.replace("//","/")

# shall we be chatty?
def verbose() :
	isverbose = getDebug() or getConfig("verbose","False")
	setCoreConfig("verbose", isverbose) # make sure this has a value downstream
	if (getDebug()) : # verbose boto
		if (not boto.config.has_section('Boto')) :
			boto.config.add_section('Boto')
		boto.config.set('Boto', 'debug', '2')            
	return("True"==isverbose)

# crazy verbose?
def getDebug() :
    return "True" == getConfig("debug","False")

# are we running in prototyping mode? 
def runLocal() :
    return (("True" == getConfig( "runLocal","False" ) ) )

# are we running X!!Tandem-style MPI implementation?
def runBangBang() :
    return (("True" == getConfig( "runBangBang", "False" ) ) )

# are we running on a hadoop cluster (and not AWS)
def runHadoop() :
    if (runLocal()) :
        return False
    return not runAWS()

# are we running on AWS (that is, not local, and not hadoop cluster)
def runAWS() :
    if (runLocal()) :
        return False
    if ( "True" == getConfig( "runAWS", "False" ) ) : #  force AWS and ignore hadoop setup?
        return True
    if ( None != getConfig("hadoop_dir",required=False) ) :
        return False  # no hadoop gateway mentioned
    return True



# are we running in old-school (not, mapreduce, not mpi) mode?
def runOldSkool() :
    value = getConfig( "oldSkool", "False" )
    setCoreConfig( "oldSkool", value ) # make sure it's initialized for use downstream
    return (("True" == value ) )

# write a timestamped message to stdout
def log(obj) :
    print ""
    log_no_newline(obj)

def log_no_newline(obj) :
    global lastlogtime
    lastlogtime = datetime.datetime.utcnow() # reset the boredom meter
       
    # deal with lists of strings, as well as simple strings
    text = ""
    if ( list == type(obj) ) :
        for o in obj:
            text = text + str(o) + " "
    else :
        text = str(obj)
    if (("."!=text) and (None == re.search("../../.. ..:..:..",text))) : # needs a timestamp?
        print lastlogtime.strftime("%y/%m/%d %H:%M:%S")+" "+text.rstrip(),  # this is the hadoop style
    else : # it IS a timestamp, or just a progress dot
        print text.rstrip(),
    

# just put a dot if we've been quiet for a while
def log_progress() :
    global lastlogtime
    if ((datetime.datetime.utcnow() - lastlogtime).seconds > 10) :
        log_no_newline('.')

# write that final newline
def log_close() :
    global logStartTime
    delta = datetime.datetime.utcnow() - logStartTime
    log( "elapsed time = "+str(delta))
    print "" # final newline

def debug(obj) :
    if (getDebug()) :
        log(obj)

def info(obj) :
    if (verbose()) :
        log(obj)

def runCommand(cmd, outputfilter=None) :
    debug(cmd)
    args = cmd.split()
    debug(args)
    return runPipeCommand(args, outputfilter)

def runPipeCommand(args, outputfilter=None) :
    chatty = getDebug() 
    if (chatty) :
        log( args )
    starttime = datetime.datetime.utcnow()
    p = subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    result = ""
    while True:
        line = p.stdout.readline()
        if (line != None) :
            line = line.rstrip()
            if (line != "") :
                if (chatty or (outputfilter and re.search(outputfilter,line))) :
                    log(line)
                if (result != "") :
                    result = result + "\n" + line
                else :
                    result = line
        if (None != p.poll()) :
            break
        # show some life on long commands - print a dot once in a while
        if (not chatty) :
            log_progress()

    ret = p.returncode
    if (ret != 0):
        log("problem running command:")
        if (not chatty) : # have we already shown what's being attempted?
            log(args)
            if (len(result)>0) :
                log( result )
            log("return code %d" % ret )
    else :
        debug( "return code %d" % ret )
    return ret


# routine to retrieve config, with error checking
def loadConfig() :
    startTime = datetime.datetime.utcnow()

    if (len(sys.argv) < 3) :
        raise RuntimeError("syntax: "+sys.argv[0]+" <cluster_config_file> <xtandem_paramters_file> [<JSON-formatted extra args string>]")

    # read the cluster config file (encoded as json, possibly with comments)
    selectCoreConfig() # cluster config is common to all potential job configs
    clusterconfig = sys.argv[1]
    if (not os.path.exists(clusterconfig)) : # not found - try same dir as the script
        withpath = os.path.join(os.path.dirname(sys.argv[0]),sys.argv[1])
        if (os.path.exists(withpath)) :
            clusterconfig = withpath
            if (not getDebug()) :
                log('using cluster config file %s' % clusterconfig)
    load_commented_json(clusterconfig)

    # note the job config file (encoded as xml in the usual X!Tandem manner, or possibly a file that is a list of files)
    setConfig( "xtandemParametersLocalPath", sys.argv[2]) # this xtandem xml config info gets processed a bit later

    # and see if there's any commandline stuff, like maybe '{"debugEMR":"True"}'
    if (len(sys.argv) >= 4) :
        args=""
        for n in range(3,len(sys.argv)) :
            args+=sys.argv[n]
            # deal with multiple json fragments, or single fragment spread across args
            if (( "{"==args[0:1]) and ("}"==args[-1:]) ) :
                log('processing extra system config info from command line: %s'%args)
                load_json_string(args)
                args=""


    info("job started")

    # we have trouble downstream if some config parameters are not set
    setConfig( "runLocal", getConfig( "runLocal", "False" ) ) 

    # use job config filename as basename if none given
    setConfig("baseName",S3CompatibleString(getConfig("baseName",os.path.basename(sys.argv[2]))))
    setConfig("coreBaseName", getConfig("baseName")) # in case of multi-param batch run

    setConfig("jobTimeStamp", startTime.strftime("%Y%m%d%H%M%S"))

    # are we running in AWS?
    if ( runAWS() ) :
        # do boto version check
        if (int(boto.Version.partition('.')[0]) < 2) :
            versionerr= "boto version 2.0 or higher is required (you have "+boto.Version+") - http://boto.googlecode.com/files/boto-2.0b1.tar.gz is known to work"
            raise RuntimeError(versionerr)

        # determine image types for head and client
        # default to small head node, large clients
        # specify region for EMR        
        setConfig("aws_region",getConfig("aws_region","us-east-1"))
        if (runBangBang()) : # head and client node types must match
            # set head type to client type if only client type was specified
            setConfig("ec2_head_instance_type",getConfig("ec2_head_instance_type",getConfig("ec2_client_instance_type",required=False),required=False))
            # else set head type to large if not specified
            setConfig("ec2_head_instance_type",getConfig("ec2_head_instance_type","m1.large"))
            setConfig("ec2_client_instance_type",getConfig("ec2_client_instance_type",getConfig("ec2_head_instance_type")))            
        else :
            setConfig("ec2_head_instance_type", getConfig("ec2_head_instance_type","m1.small"))
            setConfig("ec2_client_instance_type",getConfig("ec2_client_instance_type","m1.large"))

        # default number of tasks per client if none specified: 1 per core, (it's 2 per core in AWS EMR default)
        corecounts = { "m1.small":1,"m1.large":2,"m1.xlarge":4, "m2.large": 2, "m2.2xlarge":4, "m2.4xlarge":8,"c1.medium":2,"c1.xlarge":8, "cc1.4xlarge":8, "cg1.4xlarge":8, "cc2.8xlarge":16 }
        if ( ("" == getConfig("numberOfTasksPerClient","")) and not runLocal() ) :
            clientType = getConfig("ec2_client_instance_type")
            if (clientType in corecounts) :
                setConfig("numberOfTasksPerClient",corecounts[clientType])
            if ("" == getConfig("numberOfTasksPerClient","")) :
                unknownTaskCount = "unable to determine proper number of tasks per client for client type "+clientType
                raise RuntimeError(unknownTaskCount)
        # now establish connections to EC2 and S3
        aws_access_key_id = getConfig( "aws_access_key_id" )
        aws_secret_access_key = getConfig( "aws_secret_access_key" )
        
        aws_region = getConfig( "aws_region" )
        aws_placement = getConfig( "aws_placement", required=False ) # sub-region
        
        aws_region = RegionInfo(name=getConfig( "aws_region" ),endpoint=getConfig( "ec2_endpoint",'elasticmapreduce.amazonaws.com' ))
        global conn
        conn = boto.emr.EmrConnection(region=aws_region,aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

        # and now the connections        
        global s3handle
        global bucketName
        global s3conn
        global s3bucket
        bucketName = S3CompatibleString(getConfig("s3bucketID" ),isBucketName=True) # enforce bucket naming rules
        if ( bucketName != getConfig("s3bucketID")) :
            setConfig("s3bucketID",bucketName)
        s3conn = S3Connection(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        s3bucket = s3conn.create_bucket( bucketName )
        s3handle = Key(s3bucket)
        

def explain_hadoop() :
    # tell user to init the socks proxy
    log("!!!")
    log("Hadoop cluster access doesn't seem to be set up!")
    log("!!!")
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

def testHadoopConnection() :
     if ( runHadoop() ):
        info("testing Hadoop setup")
        if ( "Windows" == platform.system() ) :
            log("This script uses a local install of hadoop for file transfers to the cluster.")
            log("Unfortunately hadoop on Windows only works under Cygwin.")
            log("Please try again from a Cygwin shell.")
            exit(1)         
        hadoopOK = False
        try:
            args = [getHadoopBinary(),"dfs","-test","-d","hdfs://%s"%getHadoopDir()]
            hadoopOK = (0==runPipeCommand( args ))
        except Exception, exception:
            log( exception )
            
        if (not hadoopOK) :
            explain_hadoop()
            exit(1)
         
# routine to retrieve config, with error checking
def getConfig(key, default = None, required = True):
    if ( (currentCfgID >= 0) and (key in cfgStack[currentCfgID]) ) :
        return cfgStack[currentCfgID][ key ]
    if (key in cfgCore) : # in the config stuff shared by all configs in the stack?
        if (None != cfgCore[ key ]) :
            return cfgCore[ key ]
    if ( None != default ) :
        return default
    if ( required ) :
        raise Exception("no value given for %s" % key)
    return None

def getConfigToString() :
    if ( currentCfgID >= 0 ):
        return json.dumps(cfgStack[currentCfgID])+"\n"
    else :
        return json.dumps(cfgCore)+"\n"

def getConfigAsCommandlineString() :
    result = "["
    for i,cfg in enumerate(cfgStack) :
        s = "," if (i>0) else ""
        s += json.dumps(cfg)
        s = s.replace("\n","") # no newlines
        s = s.replace('"','\\"') # escape the quotes
        s = s.replace(' ','') # no spaces
        s = s.replace('\\\"\\\":\\\"\\\",','') # no empty entries
        result += s
    return result + "]"

# write all configs to a text file that we can pass to ECA for X!!Tandem use
def getConfigAsTmpfile(dirname) :
    # strip the config stack of info that can be found in the core config
    tidyConfigStack()
    result = "["
    for i,cfg in enumerate(cfgStack) :
        s = "," if (i>0) else ""
        s += json.dumps(cfg)
        s = s.replace("\n","") # no newlines
        s = s.replace(' ','') # no spaces
        s = s.replace('\"\":\"\",','') # no empty entries
        result += s
    result += "]"
    fname = dirname+"/eca_config.json"
    f=open(fname,"wb")
    f.write(result)
    f.close()    
    return fname

# write core config to a text file that we can pass to ECA for X!!Tandem use
def getCoreConfigAsTmpfile(dirname) :
    # strip the config stack of info that can be found in the core config
    tidyConfigStack()
    s = json.dumps(cfgCore)
    s = s.replace("\n","") # no newlines
    s = s.replace(' ','') # no spaces
    s = s.replace('\"\":\"\",','') # no empty entries
    s += "\n"
    fname = dirname+"/eca_core_config.json"
    f=open(fname,"wb")
    f.write(s)
    f.close()
    return fname

# set value in the current config
def setConfig(key,val) :
    if (-1 == currentCfgID) :
        setCoreConfig(key,val)
    else :
        debug("set %d %s=%s"%(currentCfgID,key,val))
        cfgStack[currentCfgID][ key ] = val

# set value shared by all configs in the stack
def setCoreConfig(key,val) :
    debug("set core %s=%s"%(key,val))
    cfgCore[ key ] = val
    for cfg in cfgStack :
        if key in cfg :
            del cfg[key]

def getJobDir() :
    if len(cfgStack) > 1 : # multi config batch job
        setConfig("jobDir", getCoreJobDir()+"/"+getConfig( "baseName" ))
        # save results to local file?  (defeated by setting "resultsFilename":"")
        if not len(getConfig("resultsFilename","")): # no results name set yet
            setCoreConfig("resultsFilename",my_abspath(getCoreJobDir()+"/results.txt"))            
        jobDir = getConfig( "jobDir" )
    else :    
        baseName = getConfig( "baseName" ) # enforce bucket naming rules
        job_ts = getConfig("jobTimeStamp")
        if ( runLocal() ) :
            jobDir = '/tmp/%s/%s' % ( baseName , job_ts )
        else :    
            jobDir = '%s_runs/%s' % ( baseName , job_ts )
        resultsDir = baseName
        setConfig( "jobDir", jobDir )
        jobDir = getConfig( "jobDir" )
        # save results to local file?  (defeated by setting "resultsFilename":"")
        if not len(getConfig("resultsFilename","")): # no results name set yet
            setCoreConfig("resultsFilename",my_abspath(jobDir+"/results.txt"))
                
    if not os.path.exists(jobDir) :
        os.makedirs(jobDir)
            
    return jobDir

def getCoreJobDir() :
    if len(cfgStack) > 1 : # multi config batch job
        baseName = getConfig( "coreBaseName" ) # enforce bucket naming rules
        job_ts = getConfig("jobTimeStamp")
        if ( runLocal() ) :
            jobDir = '/tmp/%s/%s' % ( baseName , job_ts )
        else :    
            jobDir = '%s_runs/%s' % ( baseName , job_ts )
    else :
        jobDir = getJobDir()
    return jobDir


# remove any security-sensitive settings from config then save to S3
# note that filename extension determines output format: .json or .r
def scrubAndPreserveJobConfig(filename) :
    saveConfig={"":""}
    for key,val in cfgCore.iteritems():
    	saveConfig[key] = val
    # clean out security stuff!
    setCoreConfig("aws_access_key_id", "xxxx")
    setCoreConfig("aws_secret_access_key", "xxxx")
    setCoreConfig("RSAKey", "xxxx")
    setCoreConfig("RSAKeyName", "xxxx")
    configStr = getConfigToString() + "\n" # trailing newline matters to some readers
    # escape quoted commas
    commaquote="__COMMA__QUOTE__"
    configStr = configStr.replace(",\"",commaquote)
    configStr = configStr.replace(",",",\n")
    configStr = configStr.replace(commaquote,",\"")
    # restore - we need that security stuff to connect
    for key,val in saveConfig.iteritems():
        cfgCore[key] = val
    saveStringToFile(configStr,filename)

# save the given string to the named S3 file, or locally if runLocal()
def saveStringToFile(contents,filename) :
    text = str(contents)
    full_filename = filename
    if ( runAWS() ) :
        full_filename = getConfig("s3bucketID")+"/"+filename
    try:
        if ( runLocal() ) :
            f = open(filename,"w")
            f.write(text)
            f.close()   
        else :
            set_contents_from_string(filename,text)
    except Exception, exception:
        log( exception )
        log("failed saving text to " + full_filename)
        log("exiting with error")
        exit(-1)

def setSharedDir() :
    if ( runBangBang() ) :
        setConfig("sharedDir","/mnt/")
    elif ( runAWS() ) :
        setConfig("sharedDir","/mnt/var/lib/hadoop/dfs/")
    elif ( runLocal() ) :
        setConfig("sharedDir","./")
    else :
        setConfig("sharedDir",getConfig("hadoop_dir")) # provoke an error if this isn't specified for hadoop cluster

# helps prepare a list of copy commands to be passed out to mappers
def constructHadoopCacheFileCopyCommand(cfgKey) :
    if ( runBangBang() ) :
        copierCommand = ""
        log( "shouldn't be here..." )
    else :
        hdfsname = HadoopCacheFileName(getConfig(cfgKey))
        hadoopCopyCmd = "hadoop dfs -cp "
        copierCommand = '%s s3n://%s/%s %s\n' % ( hadoopCopyCmd, bucketName, getConfig(cfgKey), hdfsname )
        setConfig(cfgKey,hdfsname) # side effect - transform filename in config for use on cluster
    return copierCommand

# make string both S3 and URL compatible
def S3CompatibleString( instr , isBucketName=False) :
    if ( runLocal() ) :
        return instr # no need for S3 style correction
    outstr = drivecaps(instr) # consistent capitalization of drive letters
    outstr = outstr.replace(":","__cln__") # TODO: deal with all illegal characters
    if (isBucketName) : # extra tight rules on the actual bucket
        outstr = outstr.lower()
        outstr = outstr.replace("_","-")
    if (instr != outstr) :
        info("note: using name \""+outstr+"\" instead of \""+instr+"\" for S3 and URL compatibility")
    return outstr

# are we running in cygwin?
def cygwin() :
    return platform.system().startswith("CYGWIN")

# strip a filename of its cygwin weirdness
def decygwinify(filename) :
    if ( filename.startswith("/cygdrive/") ) : 
        return filename[10]+":"+filename[11:]  # /cygdrive/x/foo/bar -> x:/foo/bar
    elif (cygwin()) :
        filename = os.popen("cygpath -m "+filename).readline().rstrip() # drop trailing newline
    return filename

# add cygwin weirdness to a filename if needed
def cygwinify(filename) :
    if (cygwin()) :
       if (':' == filename[1]) : # form x:/foo
           filename = os.popen("cygpath -u "+filename).readline().rstrip() # drop trailing newline
       elif (not os.path.exists(filename) and not filename.startswith("/cygdrive/") ): # prepend with /cygdrive/x/
           filename = os.popen("pwd").readline().rstrip()[:11]+"/"+filename 
    return filename

# absolute path with tidied-up path seperators

# consistent drive letter case
def drivecaps(name) :
    fname = name
    if (len(fname) > 2) and (":"==fname[1]) :
        fname = string.lower(fname[0])+fname[1:] # consistent case for drive letters
    return fname
    

# absolute path with tidied-up path seperators
def my_abspath(name) :
    fname = drivecaps( name ) # consistent case for drive letters
    # handle cygwin weirdness - c:/ is /cygdrive/c/
    if ( cygwin() ) :
        try:
            tryname = cygwinify(fname)
            if (os.path.exists(tryname)) :
                fname = tryname
        except Exception, e:
            log( e )
    fname = os.path.abspath( fname )
    if (not os.path.exists(fname)) :
        fname = name  # can happen if this gets called after pathToTargetFileSystemPath translation
    fname = drivecaps( fname )
    fname = fname.replace("\\","/")
    return fname

# take a path and turn it into a string we can use as a simple filename for HDFS cachefile purposes
def pathToTargetFileSystemPath( name ) :
    fname = my_abspath( name ) 
    if ( runBangBang() ) : # no cachefiles in X!!Tandem world, leave this as a proper path
        fname = fname.replace(":","-") # ECA style drive sep replacement
    else :
        fname = fname.replace(":","__cln__")
        fname = fname.replace("/","__sl__")
        fname = fname.replace("\\","__sl__")
    return fname

# reverse the effect of pathToTargetFileSystemPath
def filenameToPath( fname ) :
    name = fname.replace("__cln__",":")
    name = name.replace("__sl__","/")
    return name

def getLocalizeFileReferencesCmd(fname) :
    return "sed -i \"s#__sl__#/#g\" "+fname+" ; "+ \
           "sed -i \"s#__cln__#:#g\" "+fname

def constructCacheFileReference( jobDir , fileName ) :
    if ( runAWS() ) :
        return 's3n://%s/%s/%s#%s' % ( bucketName , jobDir , fileName , fileName )
    else :
        return 'hdfs://%s/%s/%s#%s' % ( getHadoopDir(), jobDir , fileName , fileName )

def getHadoopDir() :
    if (runAWS()) :
        return "/home/hadoop"
    else :
        return getConfig("hadoop_dir")

def getHadoopWorkDir() :
    if (runAWS()) :
        return "/home/hadoop"
    else :
        return getConfig("hadoop_dir")+"/"+getJobDir()

def createRemoteDir(dirname) :
    if (runHadoop()) :
        args = [getHadoopBinary(),"fs","-mkdir",dirname]
        runPipeCommand( args )

def removeRemoteFile(filename) :
    if (runHadoop()) :
        args = [getHadoopBinary(),"fs","-rm",filename]
        runPipeCommand( args )


# make name suitable for hadoop cachefile mechanism - convert path to a long string
def HadoopCacheFileName( name ) :
    if (runLocal()) :
        return name
    elif (runAWS()) :
        return "hdfs://"+getHadoopDir()+"/"+S3CompatibleString(pathToTargetFileSystemPath(name))
    else :
        return "hdfs://"+getHadoopDir()+"/"+name

# gzip a file if it needs it
def attemptGZip( inputPath ) :
    localPath = inputPath
    if (  not localPath.endswith(".gz") ) :
        # use .gz equivalent, make one if needed
        oldPath = localPath
        localPath = localPath+".gz"
        if (not os.path.exists(localPath)) :
            info( "creating gzipped copy of "+oldPath+" as "+localPath+" for transfer to S3" )
            try:
                file = open(oldPath,"rb")
                zipper = GzipFile(localPath,"wb")
                for line in file :
                    zipper.write(line)
                zipper.close()
                file.close()
            except Exception, exception:
                log( exception )
                log( "Failed to create gzipped copy of %s, using original.  This will work but is less efficient" % oldPath)
                localPath = oldPath
        else :
            info( "based on filename, "+localPath+" appears to be gzipped copy of "+oldPath+" so we'll use that" )
    return localPath

# put a file to the target system via the SOCKS proxy
def copyFileToHDFS(local_filename,target_filename,filemode="") :
    ret = -7777 # magic number used below 
    try:
        local_filename = decygwinify(local_filename) # for windows, remove cygwin hoo-hah if any
        if (not target_filename.startswith(getHadoopDir())) :
            target_filename = getHadoopDir()+"/"+target_filename
        args = [getHadoopBinary(),"fs","-put",local_filename,target_filename]
        
        if (0 != runPipeCommand( args )) :
            if (not getDebug()) :
                log( args )
            log("problem with hadoop fs -put (have you configured core-site.xml and mapred-site.xml?)")
            explain_hadoop()
            exit(ret)
        if ( "" != filemode ) :
            args = [getHadoopBinary(),"dfs","-chmod",filemode,target_filename]
            runPipeCommand( args ) 
            
    except Exception, exception:
        if (-7777 == ret) : # process never got started
            log("unable to invoke local copy of hadoop for file transfer purposes")
        log( exception )
        explain_hadoop()
        exit(1)

# run a hadoop job via the proxy (that is, on a generic Hadoop cluster, not AWS EMR)
def doHadoopStep(workstep) :
    if ( None != workstep ) :
        hadoopPath = getHadoopHome()
        hadoopVer = hadoopPath[hadoopPath.rfind("/")+1:]
        hadoopStreamingJar = decygwinify(hadoopPath+"/contrib/streaming/"+hadoopVer+"-streaming.jar")
        args = [getHadoopBinary(),"jar",hadoopStreamingJar]
        args.extend(workstep.args())
        log("running Hadoop step %s" % workstep.name)
        runPipeCommand(args, "Running job|StreamJob:  map|Job complete") # show output lines with "Running job:" in them

# put a file to the target system (S3 or hadoop cluster)
def set_contents_from_string(target_filename, contents) :
    text = str(contents)
    try:
        if (runAWS()) :
            full_filename = getConfig("s3bucketID")+"/"+target_filename
            s3handle.key = target_filename
            s3handle.set_contents_from_string(contents)
        else :
            full_filename = getHadoopDir()+"/"+target_filename
            f = tempfile.NamedTemporaryFile()
            f.write(contents)
            f.flush()
            copyFileToHDFS(f.name,target_filename)
            f.close() # self destruct
    except Exception, exception:
        log( exception )
        log("failed saving text to " + full_filename)
        log("exiting with error")
        exit(-1)
        
# put a file to the target system (S3 or hadoop cluster)
def set_contents_from_filename(target_filename, local_filename) :
    if (runAWS()) :
        s3handle.key = target_filename
        s3handle.set_contents_from_filename(local_filename)
    else :
        copyFileToHDFS(my_abspath(local_filename),getHadoopDir()+"/"+target_filename)

def copyFileFromHDFS(remoteName, localName) :
    if ( runHadoop() ):
        info("saving "+remoteName+" to " + localName)
        try:
            if (os.path.isfile(my_abspath(localName))) :
                log("overwriting existing file named "+localName)
                os.unlink(my_abspath(localName))
            localName = decygwinify(localName)
            args = [getHadoopBinary(),"fs","-copyToLocal",remoteName,localName]
            runPipeCommand( args )
        except Exception, exception:
            log( exception )
            log("failed saving "+remoteName+" to " + localName)

# read a file from S3 or hadoop cluster into a string
def get_contents_as_string(target_filename) :        
    if (runAWS()) :
        s3handle.key = target_filename
        return s3handle.get_contents_as_string()
    else :
        f = tempfile.NamedTemporaryFile(delete = False)
        f.close()
        tmpfnm = decygwinify(f.name)
        os.unlink(tmpfnm)
        copyFileFromHDFS(target_filename,tmpfnm)
        ff = open(tmpfnm, 'r')
        linestring = ff.read()
        ff.close()
        os.unlink(tmpfnm)
        return linestring

# read a file from S3 or hadoop cluster into a local file
def get_contents_to_filename(target_filename, local_filename) :
    max_retry = 3
    for retry in range(max_retry+1) :
        try:       
            if (runAWS()) :
                s3handle.key = target_filename
                s3handle.get_contents_to_filename(local_filename)
            else :
                copyFileFromHDFS(target_filename,local_filename)
            debug("copy %s to %s %d bytes" %(target_filename,local_filename,os.path.getsize(local_filename)))
            if (os.path.getsize(local_filename) > 0) :
                break
        except Exception, exception:
            pass
        if (max_retry == retry) :
            log( exception )
            log("failed saving remote file "+target_filename+" to " + local_filename)
        else :
            sleep(10)

# check list of files for existence on S3 or hadoop cluster and match with local, upload as needed
# filenameKey allows us to get at filename as config["<filenameKey>"]
# targetDir is the directory we want it to end up in (if blank, then use the directory named in config["<filenameKey>"])
# if wantGZ is true and config["<filenameKey>"] does not end in ".gz", a local gzipped copy is
# created and that's what we upload.
# if overwriteOK is True we will overwrite if needed
# filemode can be used to set file bits on hadoop
#
# side effect: config["<filenameKey>"] is changed to the S3 path for consumption by mapreduce
def uploadFile(fileNameKey, targetDir, wantGZ, overwriteOK=False, filemode="", decorate="") :

    if (runAWS()):
        from boto.s3.connection import S3Connection
        remoteStore = S3CompatibleString(getConfig("s3bucketID"),isBucketName=True) # enforce bucket naming rules

        # note: creating connection object only, no test of credentials or network here
        s3conn = S3Connection(aws_access_key_id=getConfig("aws_access_key_id"), aws_secret_access_key=getConfig("aws_secret_access_key"))

        debug( "using S3 bucket " + remoteStore + " ... " )
        try:
            s3Bucket = s3conn.create_bucket(remoteStore)
        except Exception, exception:
            log( exception )
            log( "error: unable to connect to S3 bucket " + remoteStore + ", check credentials" )
            log( "exiting with error" )
            exit(-1)
        debug( "bucket ok!" )
    elif (runHadoop()) :
        remoteStore = getHadoopDir() 
    else :
        remoteStore = ""
        
    debug( "checking for existing remote copies of data and script files" )
    localPath = getConfig(fileNameKey) # get local file path
    if ("" != localPath) :
        localPath = my_abspath( localPath )
        s3FileName = S3CompatibleString(localPath)+decorate # optionally can decorate the name on the target system
        if ("" != targetDir) :
            s3FileName = targetDir+"/"+os.path.basename(s3FileName)
        try:
            if ( wantGZ ) :
                # use .gz equivalent, make one if needed
                oldPath = localPath
                localPath = attemptGZip(localPath)
                if (oldPath != localPath) : # we gzipped it
                    s3FileName = s3FileName+".gz"
                    setConfig(fileNameKey, localPath)
                else :
                    debug( "based on filename, "+localPath+" appears to be gzipped copy of "+oldPath+" so we'll use that" )
            # now set config
            fp=open(localPath,"rb")
        except Exception, exception:
            log( exception )
            log( "error: could not open local file "+ localPath + " for upload comparison! " )
            log( "exiting with error" )
            exit(-1)
        debug( "checking for file " + s3FileName + " on remote system ... " )
        if (runAWS()) :
            testKey = s3Bucket.get_key(s3FileName)
        else : # hadoop
            try :
                hadoopName = s3FileName
                if (not hadoopName.startswith(getHadoopDir())) :
                    if (not hadoopName.startswith("/")) :
                        hadoopName = "/"+hadoopName
                    hadoopName = getHadoopDir()+hadoopName
                cmd = getHadoopBinary() + " dfs -ls "+hadoopName+" 2> /dev/null" # ignore "file not found" messages
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
          
        if ( None != testKey ):
            # file is on S3 or hadoop
            # if we have a local copy, compare against it
            needsWrite = False # probably won't need to write
                
            debug( "ok -- file found on target system" )

            # we have a local file to compare against
            # continue with verifying file

            if (runAWS()) :
                s3FileSize = testKey.size

                # get the hex digest version of the MD5 hash-- called the "etag" in aws parlance
                # (this comes back as a string surrounded by double-quote characters)
                s3HexMD5 = testKey.etag.replace('\"','')
                

                localFileSize = os.path.getsize(localPath)

                if (localFileSize != s3FileSize):
                    log( "local and S3 file sizes of "+localPath+" do not match." )
                    log( localPath+" "+ str(localFileSize)+" vs S3 copy at "+str(s3FileSize) )
                    if (overwriteOK) :
                        log( "overwriting S3 copy" )
                        needsWrite = True
                    else :
                        log( "exiting with error" )
                        exit(-1)

                debug( "calculating local md5 hash..." )
                (localHexMD5,localB64MD5) = testKey.compute_md5(fp)
                debug( "done" )
                fp.close()

                if (s3HexMD5 == localHexMD5):
                    debug( "existing S3 copy of file "+localPath+" verified with correct size and checksum, good" )
                elif (overwriteOK) :
                    log( "md5 sums for local and S3 copies of "+localPath+" did not match! Overwriting S3 copy." )
                    needsWrite = True
                else:
                    log( "error: md5 sums for local and S3 copies of "+localPath+" did not match! Exiting without overwriting file" )
                    log( "exiting with error" )
                    exit(-1)
            else : # hadoop
                localFileSize = os.path.getsize(localPath)
                if (localFileSize == testKey.size):
                    debug( "existing HDFS copy of file "+localPath+" verified with correct size, good" )
                elif (overwriteOK) :
                    log( "existing HDFS copy of file "+localPath+" is different size than local copy, updating it from local copy" )
                    needsWrite = True
                    removeRemoteFile(hadoopName) # hadoop won't overwrite existing files
                else :
                    log( "error: existing HDFS copy of file "+localPath+" is a different size!  Exiting without overwrite.")
                    exit -1

        else : # file not found on remote system
            needsWrite = True
            
        if (needsWrite) :

            # upload if possible, or exit with error otherwise
            log( "uploading " + localPath + " to " + remoteStore + "/" + s3FileName )
            if (runAWS()) :
                # boto verifies file upload itself
                fp.close()
                s3handle.key = s3FileName
                s3handle.set_contents_from_filename(localPath)
            else : # hadoop
                copyFileToHDFS(localPath, s3FileName, filemode)
                   
                    
        setConfig(fileNameKey, s3FileName) # now speak in terms of S3 for node config

def downloadFromS3(filename) :

    remoteStore = S3CompatibleString(getConfig("s3bucketID")) # enforce bucket naming rules

    # note: creating connection object only - no test of credentials or network here
    s3conn = S3Connection(aws_access_key_id=getConfig("aws_access_key_id"), aws_secret_access_key=getConfig("aws_secret_access_key"))

    try:
        s3Bucket = s3conn.create_bucket(remoteStore)
        k = Key(s3Bucket)
        k.key = filename
        return k.get_contents_as_string()
        
    except Exception, exception:
        log(exception)
        log( "error: unable to connect to S3 bucket " + remoteStore + ", check credentials" )
        log( "exiting with error" )
        exit(-1)

# extend a list of oldname,newname pairs for restoring local paths after an X!!Tandem run
translations = [("","")]
def addFilenameTranslation(originalName, newName) :
    translations.extend((originalName, newName))  # TODO use this at the end of X!! run to tidy up the result file

def calculateSpotBidAsPercentage( spotBid, instance_type, emrTax = 0.0 ) :
	demandprices = { "t1.micro" : 0.02, "m1.small" : 0.085, "m1.large" : 0.34, "m1.xlarge" : 0.68, \
			   "m2.large" : 0.50, "m2.2xlarge" : 1.00, "m2.4xlarge" : 2.00, \
			   "c1.medium" : 0.17, "c1.xlarge" : 0.68, "cc1.4xlarge" : 1.30, "cg1.4xlarge" : 2.10, \
			   "cc2.8xlarge" : 2.40}

	if ('%' in spotBid) : # a percentage, eg "25%" or "25%%"
		spotBid = "%.3f"%((1.0+emrTax)*demandprices[instance_type]*float(spotBid.rstrip("%"))*.01)
		
	return spotBid
