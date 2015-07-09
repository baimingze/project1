#!/usr/bin/python

# script run at startup by Amazon EC2 nodes in the RMPI implementation of the
#  Insilicos Ensemble Cloud Army

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
import boto.ec2
from boto.s3.connection import S3Connection
from boto.s3.key import Key

import os
import signal
import commands
import random

import logging
import datetime 
from time import sleep
from random import randrange

import simplejson as json

import subprocess
import shlex

# local file for logging
computationLogFileName =  "/var/log/eca-rmpi-computation.log"


# list of all nodes in our cluster
instancesList = []

def getInstancesList() : # find everybody that launched in this cluster, including myself
    global instancesList
    logger.info("getInstancesList()")
   
    try:
        if ( "ec2_endpoint" in parameters ) :
            aws_region = boto.ec2.connection.RegionInfo(name=parameters["aws_region"],endpoint=parameters["ec2_endpoint"])
            conn = aws_region.connect(aws_access_key_id=parameters["aws_access_key_id"], aws_secret_access_key=parameters["aws_secret_access_key"])
        else :
            conn = boto.ec2.connect_to_region(parameters["aws_region"],aws_access_key_id=parameters["aws_access_key_id"], aws_secret_access_key=parameters["aws_secret_access_key"])
            
    except Exception, exception:
        logger.error( exception )
        logger.error( "unable to connect to EC2, exiting with error" )
        exit(-1)
            
    logger.info("connected to EC2")
    if ("" == parameters["launchgroup"]) :
        # demand instances
        reservation_id = systemCmd("wget -qO- http://169.254.169.254/latest/meta-data/reservation-id", wantOutput=True).strip()
        logger.info("reservation_id %s" % reservation_id)
        reservations = conn.get_all_instances()
        for  r in reservations  :
            if ( r.id == reservation_id ) :
                instancesList = r.instances
    else :
        # spot instances
        spotreqs = conn.get_all_spot_instance_requests()
        group = parameters["launchgroup"]
        for req in spotreqs :
            if (req.launch_group == group) :
                instancesList.append(conn.get_all_instances([req.instance_id])[0].instances[0])
    logger.info("instances in this cluster:")
    for i in instancesList :
        logger.info(i)
    logger.info("getinstancelist done")
    
def myexit( exitcode ) :
    if ("head" == mode) :
        keepClients =  ( "keepClients" in parameters ) and ( "True" == parameters["keepClients"] )    
        if ( keepClients ) :
            logger.info("not terminating client nodes, per \"keepClients\" config file setting")
        else :
            logger.info("terminating client nodes")

            # who am I?
            my_instance_id = systemCmd("wget -qO- http://169.254.169.254/latest/meta-data/instance-id", wantOutput=True).strip()
            for i in instancesList: # kill everybody that isn't me
                if (my_instance_id != i.id) :
                    if "terminate" in dir(i) :  # sadly older botos say "stop" when they mean "terminate"
                        i.terminate()
                    else :  # but newer ones say "stop" when the mean "stop"
                        i.stop()

        # place full results in S3 while we wait for clients to stop
        for n in range(len(cfgStack)) :
            if ("computationLogFileName" in cfgStack[n]) :
                checkedBucketWrite( cfgStack[n]["computationLogFileName"], cfgStack[n]["reportFileName"], bucket )
            else :
                checkedBucketWrite( computationLogFileName, cfgStack[n]["reportFileName"], bucket )
                

        # also copy over any files indicated as key="resultFile_*"
        # with value="<ec2_filename>;<s3_filename>;<local_filename>"
        for key,val in parameters.iteritems():
            if (key.startswith("resultFile_")) :
                checkedBucketWrite( val.split(";")[0],val.split(";")[1], bucket)
        for n in range(len(cfgStack)) :
            for key,val in cfgStack[n].iteritems():
                if (key.startswith("resultFile_") and not key in parameters) :
                    checkedBucketWrite( val.split(";")[0],val.split(";")[1], bucket)

        
        if ( not keepClients ) :    
            # confirm all client nodes are terminated
            allTerminated = False
            numClientNodes = int(parameters["numberOfClientNodes"])
            while allTerminated != True:
                numTerminated = 0
                for i in instancesList:
                    i.update()
                    if (i.state == u'terminated'):
                        numTerminated += 1
                    
                if (int(numTerminated) == numClientNodes):
                    allTerminated = True # yay!
                    logger.info('all %d client nodes terminated' % int(numTerminated))
                else:
                    logger.info('waiting %d seconds (%d/%d terminated)' % (int(wait), int(numTerminated), numClientNodes))
                    sleep(wait)

            logger.info("client termination complete")
        
        logger.info("head node: processing complete")

        keepHead = ( "keepHead" in parameters ) and ( "True" == parameters["keepHead"] )
        if ( keepHead ) :
            logger.info("not terminating head node, per \"keepHead\" config file setting")
        else :
            logger.info('self-terminating head node. (set "keepHead":"True" in config if you do not want this)')
        # copy logfiles to S3
        checkedBucketWrite(logFileName, parameters["logFileName"], bucket)
        checkedBucketWrite("/mnt/eca-rmpi", parameters["s3JobDir"]+"/start_node.log", bucket)

        if ( not keepHead ) :
            systemCmd("shutdown -hP now")

    exit( exitcode )        

def systemCmd(cmd, expectedStatus = 0, wantOutput = False, tolerateFailure = False):
    logger.debug("running system command: %s" % cmd)

    args = shlex.split(cmd)
    cmdStatus = -99
    cmdOutput = ""

    try:
        if (wantOutput):
            process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            cmdOutput = process.communicate()[0]
            cmdStatus = process.returncode
        else:
            process = subprocess.Popen(args)
            # translate packed 16-bit C wait()-style output to shell return code
            cmdStatus = os.waitpid(process.pid, 0)[1]
    except OSError, e:
        logger.error("failed to run system command %s" % cmd)
        errStr = "  failiure:", e
        logger.error(errStr)
    
    if (cmdStatus != expectedStatus):
        logger.error("system command returned error (%d): %s" % ( int(cmdStatus), cmd ) )
        if wantOutput:
            logger.error("  command output: %s" % cmdOutput)
        if ( not tolerateFailure ) :
            logger.error("exiting with error")
            myexit(-1)
                    
    if (wantOutput):
        return cmdOutput
    else:
        return

def updatePrompt ( fname, prompt ) :
    try:
        rcFile=open(fname, "a")
        rcFile.write("\n")
        rcFile.write('PS1=\''+prompt+':\w\\$ \'')
        rcFile.close()
    except:
        # don't really care
        return 
    return

# read a file (or semicolon separated list of files)
def checkedBucketRead (semicolonSeparatedFileNameList, bucket, dataDir) :
    fileNameList = semicolonSeparatedFileNameList.split(";")
    for fileName in fileNameList :
        if None == bucket.get_key(fileName) :
            logger.error(fileName + " missing from bucket " + bucket.name)
        localFilename = dataDir + fileName
        # make sure target directory exists
        if ( not os.path.exists(os.path.dirname(localFilename))) :       
            os.makedirs(os.path.dirname(localFilename))             
        
        # it can get pretty busy at startup - do retry - randomize, wait longer for repeated failures
        maxRetries = 20
        retriesLeft = maxRetries
        while (retriesLeft > 0) :
            try:
                k=bucket.get_key(fileName)
                if k == None:
                    logger.error(fileName + " missing from bucket " + bucket.name)
                # else ok -- file found

                # get the hex digest version of the MD5 hash from S3-- called the "etag" in aws parlance
                # (this comes back as a string surrounded by double-quote characters)
                s3HexMD5 = k.etag.replace('\"','')
                s3FileSize = k.size
                k.get_contents_to_filename(localFilename)

                # calculate local hash after download
                try:
                    fp = open(localFilename, "r")
                    (localHexMD5,localB64MD5) = k.compute_md5(fp)
                    fp.close()
                    localFileSize = os.path.getsize(localFilename)
                    
                except:
                    logger.warn("error: could not open local file for comparison!")
                    raise

                if (localFileSize != s3FileSize):
                    logger.warn("local and downloaded file sizes do not match.")
                    raise Exception()

                if (s3HexMD5 != localHexMD5):
                    logger.warn("md5 sums did not match")
                    raise Exception()

                # if here, ok!
                
                retriesLeft = 0
            except:
                logger.warn( str(fileName) + ": retry due to exception on download from S3: " + str(sys.exc_info()[0]))
                sleep( randrange( maxRetries+3-retriesLeft, maxRetries+15-retriesLeft) )
                retriesLeft -= 1

        try :
            cmd = "chmod a+r " + localFilename
            os.system(cmd)
        except Exception, exception:
            logger.error( exception )
            logger.error("command \""+cmd+"\" failed")

def checkedBucketWrite ( localFileName, targetFileName, bucket, showMsg = True ) :
    try:
        if ( showMsg ) :
            logger.info( "uploading file " + localFileName + " to S3 " + targetFileName )
        k = Key( bucket )
        k.key = targetFileName
        # note: the following boto function does a md5 hash on the file before upload and verifies the resulting S3 hash.
        k.set_contents_from_filename( localFileName )
    except:
        logger.error( "error uploading file " + localFileName + " to S3 " + targetFileName )

def fileMonitor ( localFileNameList, targetFileNameList, bucket, wait ) :
    sleep( wait )
    while ( True ) :
        for ix in range( len( localFileNameList ) ) :       
            if ( os.path.isfile( localFileNameList[ ix ] ) ) :      # make sure local file exists
                checkedBucketWrite( localFileNameList[ ix ], targetFileNameList[ ix ], bucket, False )
        sleep( wait )

wait = 10   # much less than this and AWS gets irritated and throttles you back

# set up logging
# --logging code originally from http://onlamp.com/pub/a/python/2005/06/02/logging.html

#create logger
logger = logging.getLogger("")
loglevel = logging.INFO
logger.setLevel(loglevel)

# create file handler and set level
logFileName = "/var/log/eca.log"
fh = logging.FileHandler( logFileName )
fh.setLevel( loglevel )

#create formatter
formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")

#add formatter to fh
fh.setFormatter(formatter)
#add fh to logger
logger.addHandler(fh)

# log examples:
#logger.debug("debug message")
#logger.info("info message")
#logger.warn("warn message")
#logger.error("error message")
#logger.critical("critical message")

# read the config file (encoded as json)
configFileName = sys.argv[ 1 ]

# ensure config file exists
if (not os.path.isfile(configFileName)):
    logger.error("exiting: could not find userdata config file %s" % configFileName)
    exit(-1)

logger.debug("opening config file")
configfile = open(configFileName, "r")
logger.info("opened config file")

logger.debug("parsing json config file")
parameters = json.load(configfile)
configfile.close()
# unicode causes trouble downstream
for key,val in parameters.iteritems() :
    if (isinstance(val,unicode)) :
        parameters[key] = str(val.encode("ascii"))
# now check for a configuration stack - that is, multiple jobs in one run
cfgStack = []
if "cfgStack" in parameters : # multi-parameter batch job
    for n in range(len(parameters["cfgStack"])) :
        cfgStack.extend([{}])
        for key,val in parameters.iteritems(): # add in the common config params
            if key != "cfgStack" :
                cfgStack[n][key]=val
        for key,val in parameters["cfgStack"][n].iteritems() : # and any deviations
            cfgStack[n][key]=val
else :
    cfgStack.extend([{}])
    for key,val in parameters.iteritems():
        cfgStack[0][key]=val
logger.info("configuration parameters parsed")

getInstancesList(); # who all launched in this cluster?

#
# figure out if we're the head node or a client
# in demand instances we can use http://169.254.169.254/latest/meta-data/ami-launch-index
# but in spot instances every instance has launchindex=0 so we'll find all spot requests that
# share our launchgroup (which we'll only have if we are a spot bid instance)
if ("" != parameters["launchgroup"]) :
    # who am I?
    instance_id = systemCmd("wget -qO- http://169.254.169.254/latest/meta-data/instance-id", wantOutput=True).strip()
    for index, instance in enumerate(instancesList) :
        if ( instance.id == instance_id ) :
            launchindex = str(index)
            break
else :
    launchindex = systemCmd("wget -qO- http://169.254.169.254/latest/meta-data/ami-launch-index", wantOutput=True).strip()
logger.info("my launch index is %s" % launchindex)

if ("0" == launchindex) :
    mode = "head"
else :
    mode = "client"
logger.info("running as %s"%mode)

logger.debug("accessing s3")
    
s3conn = S3Connection( parameters["aws_access_key_id"], parameters["aws_secret_access_key"] )
while True:
    try:
        bucket = s3conn.create_bucket( parameters["s3bucketID"] )
        break
    except Exception, ex:
        logger.error( ex )
        logger.error( "unable to connect to S3 bucket, try again in %d seconds" % wait )
        sleep( wait )
    
# prepare for data and script copy from S3
for cfg in cfgStack :
    systemCmd( "mkdir -p " + cfg[ "dataDir" ] )

# determine name of ssh daemon script
if ( os.path.isfile("/etc/init.d/sshd") ):
    sshd_cmd = "/etc/init.d/sshd"	
else:
    sshd_cmd = "/etc/init.d/ssh"

# write the config data as R code for RMPI use
# note this handles the possibility of multiple jobs in one run
for n in range(len(cfgStack)) :
    cfgStack[n][ "computationLogFile" ] = "%s.%d"%(computationLogFileName,n)
    if (len(cfgStack)>1) :
        thisConfigFileName= "%s_%d.r"%(configFileName,n)
    else :
        thisConfigFileName= configFileName+".r"
    cfgStack[n]["configFileName"] = thisConfigFileName
    configStr = "eca_config=list("
    comma=""
    for key,val in cfgStack[n].iteritems() :
        configStr = configStr+comma+"\n\""+key+"\"=\""+str(val)+"\""
        comma=","
    configStr = configStr + "\n)\n" 
    f = open(thisConfigFileName,"w")
    f.write(configStr)
    f.close()
        

# many linux R installs muff the Rscript, pointing to wrong R install
# while we're at it, set hostfile for Open MPI (in MPICH this is done at mpiboot)
mpdhostsFileName = "/mnt/nfs-shared/mpd.hosts"
safeRscript = '/mnt/safeRscript.sh'
safeRscriptFile = open(safeRscript, "w")
safeRscriptFile.write("#!/bin/bash\n")
safeRscriptFile.write("export RHOME=$(dirname $(dirname $(which R)))\n")
safeRscriptFile.write("export OMPI_MCA_orte_default_hostfile=%s\n" % mpdhostsFileName)
safeRscriptFile.write("Rscript $1 $2 $3 $4 $5\n")
safeRscriptFile.close()
systemCmd("chmod a+x %s"%safeRscript)

s3_headIP_filename = parameters["s3JobDir"]+"/headIPAddress" # head node and clients find each other via S3 file

if ( mode == "head" ) : 
    monitorPid = os.fork()
    if ( monitorPid != 0 ) :    # parent process, continue with cluster setup
        logger.info("beginning head node mode configuration")

        # change shell prompt to reflect head node
        logger.debug("updating shell prompt")
        updatePrompt("/etc/bash.bashrc","ECA-Head-Node")
        updatePrompt("/root/.bashrc","ECA-Head-Node")
        updatePrompt("/ubuntu/.bashrc","ECA-Head-Node")
        logger.info("updated shell prompt")
         
        aws_region = parameters["aws_region"]

        numClientNodes = parameters["numberOfClientNodes"]

        rsaKey=parameters["RSAKey"]
        rsaKeyName=parameters["RSAKeyName"]
        
        numClientNodes = int(parameters["numberOfClientNodes"])
        mpiSlotsPerClient = int(parameters["numberOfRTasksPerClient"])
        mpiSlotsOnHead = int(parameters["numberOfRTasksOnHead"])
        totalNumMPISlots = mpiSlotsOnHead+(numClientNodes * mpiSlotsPerClient)
        
        logger.info("%d client nodes requested" % numClientNodes)
        logger.info("%d mpi slots per client node requested" % mpiSlotsPerClient)
        logger.info("%d mpi slots on head node requested" % mpiSlotsOnHead)
        logger.info("%d total mpi slots on cluster" % totalNumMPISlots)

        # save the ssh private key ".pem" keyfile so that ssh knows how to
        # connect to other nodes in our cluster.  Note the warnings in the
        # image configuration script about host verifecation

        logger.debug("extracting rsa key info")
        rsaFile=open("/root/.ssh/id_rsa","w")
        rsaFile.write(rsaKey)
        rsaFile.close()
        systemCmd("chmod 400 /root/.ssh/id_rsa")
        logger.info("rsa key info saved")

        # wait for public key to have been saved by other startup script
        pubKeyFileName = "/root/.ssh/authorized_keys"
        logger.info("checking public key file %s " % pubKeyFileName)
        if (not os.path.isfile(pubKeyFileName)):
            logger.info("waiting for public key")
            sleep( wait )
        logger.info("public key file %s found" % pubKeyFileName)
        
        # verify sshd (note not all implementations support "status")
        logger.info("verifying sshd")
        sshdOutput = systemCmd(sshd_cmd+" status", wantOutput=True, tolerateFailure=True)
        logger.info("sshd status: %s" % sshdOutput)

        logger.debug("getting head node ip address")
        # get the ip address of the head node (this one)
        
        head_ip_address = systemCmd("hostname -i", wantOutput = True)
        logger.info("head node ip address: %s" % head_ip_address)

        logger.info("activating nfs for sharing from head node")
        # configure an nfs shared directory: /mnt/nfs-shared
        #nfsOuput =
        os.system("/root/config_nfs_head.sh > /var/log/eca-nfs-head.log 2>&1")
        #logger.info(nfsOuput)
        logger.info("nfs activated for sharing from head node")
        # create nfs directory
        logger.debug("creating all-access client node info directory")
        systemCmd("mkdir -p /mnt/nfs-shared/clientnodeinfo")
        systemCmd("chmod -R a+rwx /mnt/nfs-shared/clientnodeinfo")
        logger.info("created all-acccess client node info directory")

        headNodeIPAddressFileName = "/mnt/nfs-shared/headIPAddress"
        logger.debug("writing head IP file")
        # save the head node's IP address
        ipFile=open(headNodeIPAddressFileName, "w")
        ipFile.write(head_ip_address)
        ipFile.close()
        systemCmd("chmod a+r %s" % headNodeIPAddressFileName)
        logger.info("wrote head node IP address file: %s" % headNodeIPAddressFileName)
        # and put it on S3 where clients can find it
        checkedBucketWrite(headNodeIPAddressFileName, s3_headIP_filename, bucket, False)

        
        logger.info("downloading R script and data files from S3 while we wait for clients to connect")
        # go through the config parameters, anything named "sharedFile_*" gets copied to data dir
        for cfg in cfgStack :
            for cfgKey,val in cfg.iteritems():
                if (cfgKey.startswith("sharedFile_")):    
                    checkedBucketRead(val, bucket, cfg["dataDir"])
            checkedBucketRead(cfg["scriptFileName"], bucket, cfg["dataDir"])
            checkedBucketRead(cfg["RMPI_FrameworkScriptFileName"], bucket, cfg["dataDir"])
            checkedBucketRead(cfg["frameworkSupportScript"], bucket, cfg["dataDir"])
            # execute the R package installation script if provided
            if "packageInstaller" in cfg :
                checkedBucketRead(cfg["packageInstaller"], bucket, cfg["dataDir"])

            n = n+1
        logger.info("S3 downloads complete")
        
        logger.info("waiting for all nodes to report ready")
        logger.info("wait delay: %d seconds" % wait)

        allBooted = False
        totalwait = 0
        maxwaitMinutes = 10 # wait 10 minutes before deciding that cluster launch is a failure
        # waiting for nodes to come online
        allReady = False
        while allReady != True:
            p1=subprocess.Popen(["ls", "-1", "/mnt/nfs-shared/clientnodeinfo/"], stdout=subprocess.PIPE)
            p2=subprocess.Popen(["wc", "-l"], stdin=p1.stdout, stdout=subprocess.PIPE)
            numready = p2.communicate()[0]
            if int(numready) != int(numClientNodes):
                logger.info('  (waiting %d seconds (%d/%d ready))' % (int(wait), int(numready), int(numClientNodes)))
                sleep(wait)
                totalwait = totalwait + wait
                if (totalwait > 60*maxwaitMinutes) : # if it doesn't happen in maxwaitMinutes its probably not happening ever
                    logger.error('only %d of %d client nodes reporting ready after %d minutes, terminating cluster'%(numready,numClientNodes,maxwaitMinutes))
                    myexit(1)
            else:
                allReady = True
                logger.info("  done waiting")

        logger.info("all %d client nodes reporting ready!" % int(numClientNodes))

        # load the client IP list into memory
        clientIPAddrList = []
        for curClient in range(0, numClientNodes):
            ipFileName="/mnt/nfs-shared/clientnodeinfo/client_%d" % int(curClient)
            ipFile=open(ipFileName, "r")
            ip = ipFile.read()
            ipFile.close()
            ip = ip.rstrip() # removing newline
            clientIPAddrList.append(ip)
            logger.info("client %d IP address: %s" % (int(curClient), ip) )
            logger.info("check:")
            cmd="ls -la %s" % ipFileName
            lsOut = systemCmd(cmd, wantOutput=True)
            logger.info("  ls: %s" % lsOut)
            cmd="cat %s" % ipFileName
            catOut = systemCmd(cmd, wantOutput=True)
            logger.info("  cat: %s" % catOut)
     
        # test the connectivity to each client
        logger.info("pinging clients")
        for curIP in clientIPAddrList:
            pingstatus = -1
            while pingstatus != 0:
                cmd = "ping -c 3 %s" % curIP
                logger.info("%s:" % cmd)
                pingstatus = os.system(cmd)
                logger.info("  output: %s" % pingstatus)
                if (pingstatus != 0):
                    logger.info("  ping failed, waiting and retrying")
                    sleep(wait)
                    totalwait = totalwait + wait
                    if (totalwait > 60*maxwaitMinutes) : # if it doesn't happen in maxwaitMinutes its probably not happening ever
                        logger.error('client at %s did not respond to ping for %d minutes, terminating cluster'%(curIP,maxwaitMinutes))
                        myexit(1)
                else:
                    logger.info("  success")
            
        # test the ssh connections

        # test SSH connection to each node
        # run the tests in parallel
        for curClient in range(0, numClientNodes):
            sshdest = "root@%s" % clientIPAddrList[curClient]
            logger.info("testing SSH to client %d (%s)" %  (int(curClient), clientIPAddrList[curClient]))
            sshcmd = "/root/sshtest.sh %s %d &" % (clientIPAddrList[curClient], int(curClient))
            os.system(sshcmd)
            
        sshSuccess = False
        while (not sshSuccess):
            countstr = commands.getoutput("grep -l 'ssh test succeeded for root@' /var/log/eca-sshtest-*.log | sort -u | wc -l")
            try :
                count = int(countstr)
            except :
                count = 0  # file not found yet, or somesuch
            if (count == numClientNodes) :
                sshSuccess = True
                logger.info("SSH to all clients succeeded")
            else:
                logger.info("%d of %d SSH clients ready, waiting and trying again" % (count, numClientNodes))
                sleep(wait)
                if (totalwait > 60*maxwaitMinutes) : # if it doesn't happen in maxwaitMinutes its probably not happening ever
                    logger.error('only %d of %d client nodes are in SSH contact afer %d minutes, terminating cluster'%(count,numClientNodes,maxwaitMinutes))
                    myexit(1)

        # assemble head, client IPs into mpd.hosts file and start MPI

        # create hosts file as node:slots
        logger.debug("building mpd hosts file")
        mpdhostsFile = open(mpdhostsFileName, "w")
        head_ip_address = head_ip_address.rstrip() # removing any possible newline
        mpiOutput = systemCmd("mpirun --version", wantOutput = True,tolerateFailure = True)
        openMPI = ( mpiOutput.find("Open MPI") >= 0 )
        # OpenMPI seems to treat slots as meaning "slaves", so for head node
        # slots=8 will get you 9 processes - Rank0 and Ranks 1-7
        if (openMPI) :
            hostfile_format = "%s slots=%d\n"
            if ( ( 0 == numClientNodes ) and ( 0 == mpiSlotsOnHead ) ) :
                mpiSlotsOnHead = 1 # watch the single node single core case
        else :
            hostfile_format = "%s:%d\n"
            mpiSlotsOnHead = mpiSlotsOnHead + 1 
        mpdhostsFile.write(hostfile_format % (head_ip_address,int(mpiSlotsOnHead)))
        for curClient in range(0, numClientNodes):
            mpdhostsFile.write(hostfile_format % (clientIPAddrList[curClient], int(mpiSlotsPerClient)))
        mpdhostsFile.close()
        logger.info("mpd hosts file assembled")
        if (not openMPI) : # not openmpi (which doesn't have mpdboot, mpdtrace, etc)
            logger.info("starting mpi service")
            mpiOutput = systemCmd("/etc/init.d/eca-mpdboot.sh /mnt/nfs-shared/mpd.hosts", wantOutput = True)
            logger.info(mpiOutput)
            if ( mpiOutput.find("failed") >= 0) :
                logger.error("MPI service did not start properly, terminating cluster")
                myexit(1)
            logger.info("MPI service started successfully!")

        logger.info("head node: configuration complete")
        
        now = datetime.datetime.utcnow()

        # run ensemble R script
        for cfg in cfgStack :
            computationLogFileName = cfg[ "computationLogFile" ]
            computationLogFile = open( computationLogFileName, "w" )
            reportFileName = cfg[ "reportFileName" ]      
            RMPI_FrameworkScriptFileName = cfg[ "dataDir" ] + cfg["RMPI_FrameworkScriptFileName"]
            # execute the R package installation script if provided
            if "packageInstaller" in cfg :
                packageInstaller = cfg[ "dataDir" ] + cfg["packageInstaller"]
                args = [ safeRscript, packageInstaller ]
                logger.debug( "running R package installer script \"%s\"" % ( packageInstaller ) )
                runRScript = subprocess.Popen( args, cwd = cfg[ "dataDir" ] )
                runRScript.wait()
            args = [ safeRscript, RMPI_FrameworkScriptFileName, cfg["configFileName"] ]
            logger.info( "running R script \"%s %s\": check S3 bucket %s/%s for results" % ( RMPI_FrameworkScriptFileName, cfg["configFileName"], parameters["s3bucketID"], reportFileName ) )
            runRScript = subprocess.Popen( args, stdout = computationLogFile, stderr = subprocess.STDOUT, cwd = cfg[ "dataDir" ] )
            runRScript.wait()
            computationLogFile.close()
 
        os.kill( monitorPid, signal.SIGTERM )

        myexit(0)
	
    else :
        # child process, start file monitor to continuously copy log and results files
        # to bucket so they can be retrieved and displayed by local client
        localFileNameList = [ logFileName ]
        targetFileNameList = [ parameters[ "logFileName" ] ]
        for cfg in cfgStack :
            localFileNameList.extend([ cfg["computationLogFile"] ] )
            targetFileNameList.extend([ cfg["reportFileName"] ] )
        fileMonitor( localFileNameList, targetFileNameList, bucket, wait )

elif mode == "client":
    logger.info("beginning client node mode configuration")


    logger.info("downloading R script and data files from S3")
    # go through the config parameters, anything named "sharedFile_*" gets copied to data dir
    for cfg in cfgStack :
        for cfgKey,val in cfg.iteritems():
            if (cfgKey.startswith("sharedFile_")):    
                checkedBucketRead(val, bucket, cfg["dataDir"])
        # execute the R package installation script if provided
        if "packageInstaller" in cfg :
            checkedBucketRead(cfg["packageInstaller"], bucket, cfg["dataDir"])
            packageInstaller = cfg[ "dataDir" ] + cfg["packageInstaller"]
            args = [ safeRscript, packageInstaller ]
            logger.debug( "running R package installer script \"%s\"" % ( packageInstaller ) )
            runRScript = subprocess.Popen( args,  cwd = cfg[ "dataDir" ] )
            runRScript.wait()
                
    logger.info("S3 downloads complete")

    # find the head node
    while (None == bucket.get_key(s3_headIP_filename)) :
        logger.info("waiting for head node to report its IP address in S3 bucket")
        sleep( wait )
    head_ip_addr = bucket.get_key(s3_headIP_filename).get_contents_as_string()
    
    logger.info("head node IP address: %s" % head_ip_addr)
    client_id = int(launchindex)-1
    logger.info("client ID: %d" % client_id)
    rsaKey=parameters["RSAKey"]

    # save the ".pem" keyfile so that ssh knows how to connect to
    # other nodes in our cluster.  Note the security warnings in the
    # image configuration script.

    # save ssh private key
    # (do this first, just in case that matters)
    logger.debug("extracting rsa key info")

    rsaFile=open("/root/.ssh/id_rsa","w")
    rsaFile.write(rsaKey)
    rsaFile.close()
    systemCmd("chmod 400 /root/.ssh/id_rsa")
    logger.info("rsa key info saved")

    # wait for public key to have been saved by other startup script
    pubKeyFileName = "/root/.ssh/authorized_keys"
    logger.info("checking public key file %s " % pubKeyFileName)
    if (not os.path.isfile(pubKeyFileName)):
        logger.info("waiting for public key")
        sleep( wait )
    logger.info("public key file %s found" % pubKeyFileName)

    # verify sshd (note not all implementations support "status")
    logger.info("verifying sshd")
    sshdOutput = systemCmd(sshd_cmd+" status", wantOutput=True, tolerateFailure=True)
    logger.info("sshd status: %s" % sshdOutput)

    # restart sshd for good measure
    logger.info("restarting sshd")
    sshdOutput = systemCmd(sshd_cmd+" restart", wantOutput=True)
    logger.info("sshd restart status: %s" % sshdOutput)

    # verify sshd (note not all implementations have "status")
    logger.info("verifying sshd")
    sshdOutput = systemCmd(sshd_cmd+" status", wantOutput=True, tolerateFailure=True)
    logger.info("sshd status: %s" % sshdOutput)

    # change shell prompt to reflect client mode
    logger.debug("updating shell prompt")

    updatePrompt("/etc/bash.bashrc", "ECA-Client-Node")
    updatePrompt("/root/.bashrc", "ECA-Client-Node")
    updatePrompt("/ubuntu/.bashrc", "ECA-Client-Node")
    logger.info("updated shell prompt")


    # get the ip address of the client node (this one)
    logger.info("getting local ip address")

    client_ip_address = systemCmd("hostname -i", wantOutput = True)
    logger.info("client node IP address: %s" % client_ip_address)

    # set up nfs shared directory for the client
    logger.info("starting nfs service as client")
    
    #nfsOutput =
    os.system("/root/config_nfs_client.sh %s > /var/log/eca-nfs-client.log 2>&1" % head_ip_addr)
    logger.info("running command: /root/config_nfs_client.sh %s" % head_ip_addr)
    # logger.info("  output: %s" % nfsOutput)
    logger.info("nfs service (client mode) started")

    # save the client node's IP address for others to find:
    ipFileName="/mnt/nfs-shared/clientnodeinfo/client_%d" % client_id
    logger.info("logging this ip and checking in: %s" % ipFileName)
    ipFile=open(ipFileName, "w")
    ipFile.write(client_ip_address)
    ipFile.close()
    systemCmd("chmod a+r " + ipFileName)
    # note-- this is also the semaphore that we're ready to proceed!
    logger.info("client IP info saved")

    logger.info("client node %d: configuration complete" % client_id)
else:
    logger.error("error: unrecognized cluster node mode")

