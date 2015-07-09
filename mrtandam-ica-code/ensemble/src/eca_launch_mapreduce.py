#!/opt/local/bin/python

# script for launching ensemble learning jobs in Amazon Elastic Map Reduce
# Copyright (C) 2010 Insilicos LLC  All Rights Reserved
# Original Authors Jeff Howbert, Natalie Tasman, Brian Pratt
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#

#
# general idea is to launch a framework R script which sources a configurable
# script that contains the various bits of mapreduce code

#
# expected file layout when this all runs:
# <your bucket>
# <your bucket>/<path_to_trainingDataFile>
# <your bucket>/<path_to_testDataFile>
# <your bucket>/<baseNameFromConfigFile>
# <your bucket>/<baseNameFromConfigFile>/<timestamp>/ (the "job directory")
# <your bucket>/<baseNameFromConfigFile>/<timestamp>/<configFile>
# <your bucket>/<baseNameFromConfigFile>/<timestamp>/<scriptFile>
# <your bucket>/<baseNameFromConfigFile>/<timestamp>/results/<mapReduce results file(s)>

import sys
import os.path
import boto.ec2
import boto.s3
import boto.emr
from boto.emr import BootstrapAction
from boto.ec2.regioninfo import RegionInfo
from boto.emr.step import StreamingStep
from boto.emr.connection import EmrConnection
from boto.s3.connection import S3Connection
from boto.s3.bucketlistresultset import BucketListResultSet
import eca_launch_helper as eca # functions commont to RMPI and MapReduce versions


from boto.s3.key import Key
import simplejson as json
from time import sleep
import datetime 

eca.loadConfig("mapreduce") # get config as directed by commandline, mapreduce style
jobDir = eca.getCoreJobDir() # gets baseName, or umbrella name for multi-config batch job
jobDirS3 = eca.S3CompatibleString(jobDir)

syspath=os.path.dirname(sys.argv[0])
if (""==syspath) :
    syspath = os.getcwd()
syspath = syspath.replace("\\","/") # tidy up any windowsy slashes

eca.setCoreConfig("mapReduceFrameworkScript", eca.getConfig( "mapReduceFrameworkScript",syspath+"/eca_mapreduce_framework.R"))
eca.setCoreConfig("frameworkSupportScript", eca.getConfig( "frameworkSupportScript",syspath+"/eca_common_framework.R"))
# are we running on AWS Elastic MapReduce? (could be a generic hadoop cluster, instead)
if ( eca.runAWS() ) :
    aws_access_key_id = eca.getConfig( "aws_access_key_id" )
    aws_secret_access_key = eca.getConfig( "aws_secret_access_key" )
    
    aws_region = eca.getConfig( "aws_region" )
    aws_placement = eca.getConfig( "aws_placement", required=False ) # sub-region
    
    aws_region = RegionInfo(name=eca.getConfig( "aws_region" ),endpoint=eca.getConfig( "ec2_endpoint",'elasticmapreduce.amazonaws.com' ))
    conn = boto.emr.EmrConnection(region=aws_region,aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)


else :
    conn = eca.HadoopConnection()
head_instance_type = eca.getConfig( "ec2_head_instance_type" )
client_instance_type = eca.getConfig( "ec2_client_instance_type" )
# optional: name of existing EC2 keypair for SSH to head node
ec2KeyPair = eca.getConfig( "RSAKeyName", required=False )

# prepare a list of files to be copied from S3 to where the clients can access them
if (eca.runLocal()) :
    eca.setCoreConfig("sharedDir",jobDir + "/")  
elif ( eca.runAWS() )  :    
    bucketName = eca.S3CompatibleString(eca.getConfig("s3bucketID" ),isBucketName=True) # enforce bucket naming rules
    bucketURL = "s3n://"+bucketName
    # directory for passing large key values in files
    eca.setCoreConfig("sharedDir","/mnt/var/lib/hadoop/dfs/")
    s3conn = S3Connection(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    s3bucket = s3conn.create_bucket( bucketName )
    k = Key(s3bucket)
else :
    bucketName = 'hdfs://%s' % eca.getHadoopDir()
    bucketURL = bucketName
    k = eca.HadoopConnection()

# write the framework and implementation scripts to
# per-job directory as a matter of record
frameworkScriptPath = eca.getConfig( "mapReduceFrameworkScript")
frameworkSupportScriptPath = eca.getConfig( "frameworkSupportScript")
mapReduceScriptPath = eca.getConfig( "scriptFileName" )
baseName = eca.getConfig( "baseName" )
configName = '%s.cfg.r' % baseName

if ( not eca.runLocal() ) :
    frameworkScriptName = eca.S3CompatibleString(os.path.basename(frameworkScriptPath))
    k.key = '%s/%s' % ( jobDirS3 , frameworkScriptName )
    k.set_contents_from_filename(frameworkScriptPath)
    eca.makeFileExecutable(k.key) 
    frameworkSupportScriptName = eca.S3CompatibleString(os.path.basename(frameworkSupportScriptPath))
    k.key = '%s/%s' % ( jobDirS3 , frameworkSupportScriptName )
    eca.setCoreConfig( "frameworkSupportScript", frameworkSupportScriptName) # use the version without path info
    k.set_contents_from_filename(frameworkSupportScriptPath)
    scriptName = eca.S3CompatibleString(os.path.basename(mapReduceScriptPath))
    k.key = '%s/%s' % ( jobDirS3 , scriptName )
    k.set_contents_from_filename(mapReduceScriptPath)
    # now we can refer to these without a path
    eca.setCoreConfig( "mapReduceFrameworkScript", frameworkScriptName )
    eca.setCoreConfig( "frameworkSupportScript", frameworkSupportScriptName)
    eca.setCoreConfig( "scriptFileName", scriptName)
    configName = os.path.basename(configName)

    configCache = eca.constructCacheFileReference( bucketName , jobDirS3 , configName )
    frameworkCache = eca.constructCacheFileReference( bucketName , jobDirS3 , frameworkScriptName)
    scriptCache = eca.constructCacheFileReference( bucketName , jobDirS3 , scriptName )
    scriptSupportCache = eca.constructCacheFileReference( bucketName , jobDirS3 , frameworkSupportScriptName )
    cachefiles = [ configCache, frameworkCache, scriptCache, scriptSupportCache ]
    # create a job step to copy data from S3 to HDFS 
    copierInputFile = '%s/copier-input-values' % jobDirS3
    copierCommands = ""
    # go through the config parameters, anything named "sharedFile_*" gets uploaded
    # to S3 with a gzip preference
    for n in range(-1,len(eca.cfgStack)) :
        for cfgKey,val in eca.selectConfig(n).iteritems():
            if (cfgKey.startswith("sharedFile_")):
                fullLocalPath = eca.my_abspath( val )              # convert relative path to absolute
                eca.setConfig( cfgKey, fullLocalPath)
                # do the upload to S3  
                s3FileList = [(cfgKey, n, "", True)]
                eca.uploadToS3(s3FileList) # side effect: after this call config speaks of data files in terms of S3
                # and set up for copying S3 files out to HDFS
                hdfsPath = "hdfs:///home/hadoop/"
                hdfsname = hdfsPath+os.path.basename(eca.getConfig(cfgKey))
                hadoopCopyCmd = "hadoop dfs -cp "
                # prepare a list of copy commands to be passed out to mappers
                cmd = '%s %s%s %s\n' % ( hadoopCopyCmd, bucketURL, eca.getConfig(cfgKey), hdfsname )
                if not cmd in copierCommands :
                    copierCommands = copierCommands + cmd
                eca.setConfig(cfgKey,hdfsname)
    k.key = copierInputFile
    k.set_contents_from_string(copierCommands)

# are we planning a spot bid instead of demand instances?
spotBid = eca.getConfig("spotBid","")
if ("" != spotBid) :
    if ('%' in spotBid) : # a percentage, eg "25%" or "25%%"
        spotBid = eca.calculateSpotBidAsPercentage( spotBid, client_instance_type, 0.20 ) # about 20% more for EMR instances
    launchgroup = "ECA"+eca.getConfig( "baseName" ) +"_"+eca.getConfig("jobTimeStamp")
else :
    launchgroup = ""
eca.setCoreConfig("launchgroup",launchgroup)

# mapper keys are random seeds
# there is only one reducer key
# mapper input file is just a list of integers 0 through (ensembleSize-1)
mapperInputFile = '%s/mapper-input-values' % jobDirS3
mapperInputs = ""
for count in range(int(eca.getConfig( "ensembleSize" ))) :
    mapperInputs = mapperInputs + str(count) + "\n"
eca.saveStringToFile(mapperInputs,mapperInputFile)

# write parameters to for the record (after removing security info)
eca.scrubAndPreserveJobConfig( '%s/%s' % ( jobDirS3 , configName ) )

# and now execute
if (eca.runLocal()) :
    # execute the package installer script
    packageInstallerScriptText=eca.create_R_package_loader_script(eca.getConfig("scriptFileName"))
    eca.setCoreConfig("packageInstaller", '%s/%s.installpackages.r' % ( jobDirS3, configName ))
    eca.saveStringToFile(packageInstallerScriptText, eca.getConfig("packageInstaller"))
    cmd = "Rscript " + eca.getConfig("packageInstaller")
    eca.log( "run: " + cmd )
    os.system(cmd)
    
    configName = '%s/%s' % ( jobDirS3 , configName )
    for n in range(0,len(eca.cfgStack)) :
        eca.selectConfig(n)
        resultsFilename=eca.getConfig("resultsFilename")
        subCfgName = eca.getConfig("eca_uniqueName")
        mapResults=resultsFilename+"."+subCfgName+".map" 
        redResults=resultsFilename+"."+subCfgName+".red" 
        mapper = "Rscript %s  mapper %s %s %s" % (frameworkScriptPath,mapReduceScriptPath,configName,subCfgName)
        if (resultsFilename != "") :
            mapper=mapper+" 2>"+mapResults  # capture logging on stderr
        reducer = "Rscript %s reducer %s %s %s" % (frameworkScriptPath,mapReduceScriptPath,configName,subCfgName)
        if (resultsFilename != "") :
            reducer=reducer+" >"+redResults +" 2>&1" # capture logging on stderr as well as results on stdout
        cmd = "cat " + mapperInputFile + " | " + mapper + " | sort | " + reducer
        eca.log("run: "+ cmd)
        os.system(cmd)
        wait = 1
        if (resultsFilename != "") :
            os.system("cat "+mapResults +" >> "+resultsFilename) # combine mapper and reducer logs
            os.system("cat "+redResults +" >> "+resultsFilename) # combine mapper and reducer logs
            os.system("cat "+mapResults) # display mapper logs
            os.system("cat "+redResults) # display reducer logs
            os.system("rm "+mapResults) # delete mapper log
            os.system("rm "+redResults) # delete reducer log

    
else :
    # bootstrap actions to customize EMR image for our purposes - no need to run on master
    bootstrapText = '#!/bin/bash\n'
    if ("True" == eca.getConfig("update_EMR_R_install","False")) :
        # get latest R (creaky old 2.7 is default on EMR)
        bootstrapText = bootstrapText + '# select a random CRAN mirror\n'
        bootstrapText = bootstrapText + 'mirror=$(sudo Rscript -e "m=getCRANmirrors(all = TRUE) ; m[sample(1:dim(m)[1],1),4]" | cut -d "\\"" -f 2)\n'
        bootstrapText = bootstrapText + 'echo "deb ${mirror}bin/linux/debian lenny-cran/" | sudo tee -a /etc/apt/sources.list\n'
        bootstrapText = bootstrapText + '# hose out old pre-2.10 R packages\n'
        bootstrapText = bootstrapText + 'rpkgs="r-base r-base-dev r-recommended"\n'
        bootstrapText = bootstrapText + 'sudo apt-get remove --yes --force-yes r-cran-* r-base* $rpkgs\n'
        bootstrapText = bootstrapText + '# install fresh R packages\n'
        bootstrapText = bootstrapText + 'sudo apt-get update\nsudo apt-get -t lenny-cran install --yes --force-yes $rpkgs\n'
    # and make sure any packages mentioned in the user script are present
    bootstrapText = bootstrapText + 'cat >/tmp/installPackages.R <<"EndBlock"\n'
    bootstrapText = bootstrapText + eca.create_R_package_loader_script(eca.getConfig("scriptFileName"))
    bootstrapText = bootstrapText + "EndBlock\nsudo Rscript /tmp/installPackages.R\nexit $?\n"
    bootstrapFile = "bootstrap.sh"
    eca.debug("writing AWS EMR bootstrap script to %s" % bootstrapFile)
    k.key = '%s/%s' % ( jobDirS3, bootstrapFile )
    k.set_contents_from_string(bootstrapText)
    bootstrapActionInstallRPackages = BootstrapAction("install R packages",'s3://elasticmapreduce/bootstrap-actions/run-if', ['instance.isMaster!=true','s3://%s/%s' % (bucketName, k.key)])
    
    copierScript = '%s copier' % ( frameworkScriptName )
    mapperScript = '%s mapper %s %s' % ( frameworkScriptName , scriptName, configName )
    reducerScript = '%s reducer %s %s' % ( frameworkScriptName , scriptName, configName )
    
    # write results here
    eca.log("scripts, config and logs will be written to %s/%s" % (bucketURL,jobDirS3))

    # tell Hadoop to run just one reducer task, and set mapper task count in hopes of giving reducer equal resources
    nodecount = int(eca.getConfig( "numberOfClientNodes", eca.getConfig( "numberOfNodes", 0 ) )) # read old style as well as new
    if (nodecount < 1) :
        nodecount = 1 # 0 client nodes means something in RMPI, but not Hadoop
    mapTasksPerClient = int(eca.getConfig("numberOfRTasksPerClient")) 
    nmappers = (nodecount*mapTasksPerClient)-1 # -1 so reducer gets equal resources
    stepArgs = ['-jobconf','mapred.task.timeout=1200000','-jobconf','mapred.reduce.tasks=1','-jobconf','mapred.map.tasks=%d' % nmappers]
    workstepsStack = []
    for n in range(0,len(eca.cfgStack)) :
        worksteps = []
        if ((0==n) and eca.runAWS()) :
            # specify a streaming (stdio-oriented) step to copy data files from S3
            copierStep = boto.emr.StreamingStep( name = '%s-copyDataFromS3toHDFS' % baseName,
                                       mapper = '%s' % (copierScript),
                                       reducer = 'NONE',
                                       cache_files = cachefiles,                                         
                                       input = '%s/%s' %  (bucketURL, copierInputFile),
                                       output = '%s/%s/copierStepResults' %  (bucketURL, jobDir),
                                       step_args = ['-jobconf','mapred.task.timeout=1200000']) # double the std timeout for file transfer
            worksteps.extend( [copierStep] )
        eca.selectConfig(n)
        eca.setConfig("completed",False,noPostSaveWarn=True)
        subCfgName = eca.getConfig("eca_uniqueName")
        eca.setConfig("resultsDir", '%s/%s' % (jobDir,subCfgName),noPostSaveWarn=True)
        # specify a streaming (stdio-oriented) step
        if (baseName == subCfgName) :
            stepname = baseName
        else :
            stepname = '%s-%s' % (baseName, subCfgName)
        workstep = boto.emr.StreamingStep( name = stepname,
                                   mapper = '%s %s' % (mapperScript, subCfgName),
                                   reducer = '%s %s' %  (reducerScript, subCfgName),
                                   cache_files = cachefiles,
                                   input = '%s/%s' %  (bucketURL, mapperInputFile),
                                   output = '%s/%s' %  (bucketURL, eca.getConfig("resultsDir")),
                                   step_args = stepArgs)
        worksteps.extend([workstep])
        workstepsStack.extend([worksteps])

    # and run the job
    keepalive = ("True" == eca.getConfig("keepHead","False"))
    if ( keepalive ) :
        failure_action = 'CANCEL_AND_WAIT'
    else :
        failure_action = 'TERMINATE_JOB_FLOW'    

    if ("" != spotBid) :
        from boto.emr.instance_group import InstanceGroup  # spot EMR is post-2.0 stuff - 2.1rc2 is known to work
        launchGroup = eca.getConfig("launchgroup")
        instanceGroups = [  
            InstanceGroup(1, 'MASTER', head_instance_type, 'SPOT', 'master-%s' % launchGroup, spotBid),  
            InstanceGroup(nodecount, 'CORE', client_instance_type, 'SPOT', 'core-%s' % launchGroup, spotBid)  
            ]
        jf_id = conn.run_jobflow(name = baseName,
                                log_uri='s3://%s/%s' %  (bucketName, jobDir),
                                ec2_keyname=ec2KeyPair,
                                action_on_failure=failure_action,
                                keep_alive=keepalive,
                                instance_groups=instanceGroups,
                                enable_debugging=("False"==eca.getConfig("noDebugEMR","False")),
                                steps=workstepsStack[0],
                                bootstrap_actions=[bootstrapActionInstallRPackages])
        
    else :
        jf_id = conn.run_jobflow(name = baseName,
                            log_uri='s3://%s/%s' %  (bucketName, jobDir),
                            ec2_keyname=ec2KeyPair,
                            action_on_failure=failure_action,
                            keep_alive=keepalive,
                            master_instance_type=head_instance_type,
                            slave_instance_type=client_instance_type,
                            enable_debugging=("False"==eca.getConfig("noDebugEMR","False")),
                            num_instances=(nodecount+1), # +1 for master
                            steps=workstepsStack[0],
                            bootstrap_actions=[bootstrapActionInstallRPackages])
    for n in range(1,len(workstepsStack)) : # adding all multi-config steps at once can overwhelm boto
        conn.add_jobflow_steps(jf_id,workstepsStack[n])

    wait = 10 # much less than this and AWS gets irritated and throttles you back
    lastState = ""
    while True:
        jf = conn.describe_jobflow(jf_id)
        if (lastState != jf.state) : # state change
            eca.log_no_newline("cluster status: "+jf.state)
            lastState = jf.state
        else :
            eca.log_progress() # just put a dot
        for n in range(0,len(eca.cfgStack)) :
            eca.selectConfig(n)
            if (not eca.getConfig("completed")) :
                # grab the results
                concat = ""
                mask = '%s/part-' % eca.getConfig("resultsDir")
                eca.debug("checking %s"%mask)
                if ( eca.runAWS() ) :
                    for part in BucketListResultSet(s3bucket, prefix=mask) :
                        # all results in one string
                        k.key = part
                        concat = concat + k.get_contents_as_string()
                else : # hadoop
                    k.key = mask+"*"
                    concat = k.get_contents_as_string()
                if (len(concat) > 0) :
                    eca.log("Done.  Results:")
                    eca.log(concat)   
                    # write to file?
                    resultsFilename=eca.getConfig("resultsFilename")
                    if (resultsFilename != "") :
                        f = open(resultsFilename,"w+")
                        f.write(concat)
                        f.close()
                        eca.log('results also written to %s' % resultsFilename)
                    eca.setConfig("completed",True,noPostSaveWarn=True)
                    lastState = '' # just to provoke reprint of state on console
        if lastState == 'COMPLETED':
            break
        if lastState == 'FAILED':
            break
        if lastState == 'TERMINATED':
            break
        sleep(wait)

eca.log_close()
