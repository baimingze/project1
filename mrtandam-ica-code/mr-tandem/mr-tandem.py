#!/opt/local/bin/python2.5

# script for launching X!Tandem jobs in Amazon Elastic Map Reduce or private
# Hadoop clusters
#
# also can be used to launch X!!Tandem (MPI-parallelized) jobs in AWS EC2 by
# leveraging the Ensemble Cloud Army work at http://sourceforge.net/projects/ica/
#
# Part of the Insilicos Cloud Army Project:
# see http://sourceforge.net/projects/ica/trunk for latest and greatest
#
# Usage:
# xtandem_mapreduce.py <cluster_config_file> <xtandem_config_file> <additional_cluster_configs>
# xtandem_config_file can be a standard X!Tandem parameter file, or a file containing
# a list of parameter files, one file per line.
#
# Copyright (C) 2010,2011 Insilicos LLC  All Rights Reserved
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
import os.path
import mapreduce_helper as mrh # mapreduce setup functions
import platform
import shutil

from xml.etree import ElementTree as ET # for picking through the X!Tandem config file to find filenames


import simplejson as json
from time import sleep
import time
import datetime

import boto.ec2
import boto.s3
import boto.emr
from boto.emr.step import StreamingStep


mrh.loadConfig() # get config as directed by commandline
syspath=os.path.dirname(sys.argv[0]).replace("\\","/") # tidy up any windowsy slashes

if mrh.getDebug() :
    mrh.log("debug mode on")
elif mrh.verbose() :
    mrh.log("verbose mode on")

# prepare a list of files to be copied from S3 to where the clients can access them
jobDir = mrh.getJobDir()
if (mrh.runHadoop()) :
    # driving a bare Hadoop cluster, as opposed to using the AWS EMR API
    mrh.setConfig("sharedDir","")
    baseURL = "hdfs://"+mrh.getHadoopDir()
    mrh.testHadoopConnection()

    
elif (not mrh.runLocal()) :
    from boto.ec2.regioninfo import RegionInfo
    from boto.emr.connection import EmrConnection
    from boto.s3.bucketlistresultset import BucketListResultSet
    from boto.emr import BootstrapAction
    
    # directory for passing large key values in files
    mrh.setSharedDir()
    bucketName = mrh.bucketName
    baseURL = "s3n://"+bucketName
else :
    mrh.setSharedDir()
    
#
# handle xtandem paramters
# note that this arg can either be a param file, or a file with a list of param files
# to facilitate multiple file searches in a single cluster session

xtandemParametersLocalPath = mrh.getConfig("xtandemParametersLocalPath")
jobName = mrh.getConfig("baseName")
jobDirMain = mrh.getCoreJobDir() # gets baseName, or umbrella name for multi-config batch job
tandem_url="" # helps with non-EMR hadoop
tandem_link="" # helps with non-EMR hadoop
nmappers = -1
configfiles = []
try:
    for line in open(xtandemParametersLocalPath,'r') :
        if ("<?xml" in line) or ("<bioml>" in line) :  # a regular xtandem config file
            configfiles = [xtandemParametersLocalPath]
            mrh.pushConfig() # add a single config
            break
        else : # assume each line is the name of an xtandem config file
            line = line.strip()
            if (len(line) > 0) :
                mrh.log("adding parameters file %s to run" % line)
                configfiles.extend([line])
                mrh.pushConfig() # add another config to batch run
except Exception, inst:
    mrh.log( "Unexpected error opening X!Tandem parameters file %s: %s" % (xtandemParametersLocalPath, inst) )
    if (not os.path.exists(xtandemParametersLocalPath)) :
        mrh.log("quitting")
        exit(1)

nSharedFileIDs = 1
nParamFiles = 0
cachefilesStack = []
workstepsStack = []
for xtandemParametersLocalPath in configfiles: # peruse each config file and do needed setup and file xfers
    mrh.info("begin processing parameters file %s" % xtandemParametersLocalPath)
    mrh.selectConfig(nParamFiles)
    # different jobdirs for different jobs in a multi-paramfile setup
    mrh.setConfig("baseName",os.path.basename(xtandemParametersLocalPath))
    mrh.setConfig("eca_cfgName",mrh.getConfig("baseName")) # helpful in x!!tandem 
    mrh.setConfig("xtandemParametersLocalPath",xtandemParametersLocalPath)
    nParamFiles = nParamFiles+1
    jobDir = mrh.getJobDir()
    #
    # write the xtandem parameters file to the per-job directory as a matter of record
    # but first fix up its internal references for S3 use
    # note this loop structure is for support of nested param files
    xtandemParametersLocalPathList = [ mrh.getConfig("xtandemParametersLocalPath") ]
    cachefiles = [] # we'll construct a list of symlinks for tandem's use
    mrh.setConfig("refineSetting","") # assume no refine step unless config shows otherwise
    defaultparamsLocalPath = ""
    taxonomyLocalPath = ""
    proteintaxon = ""
    spectrumName = ""
    outputLocalPath = ""
    outputS3CompatibleName = ""
    databaseRefs = []
    mainXtandemParametersName = mrh.S3CompatibleString(mrh.pathToTargetFileSystemPath(xtandemParametersLocalPathList[0]))
    mrh.setConfig("mainXtandemParametersName",mainXtandemParametersName)
    paramsSuffix = "" # first pass is for handling main params file
    for xtandemParametersLocalPath in xtandemParametersLocalPathList : # this loop handles nested param files
        xtandemParametersName = mrh.S3CompatibleString(mrh.pathToTargetFileSystemPath(xtandemParametersLocalPath))
        # now examine to xtandem parameters file to see what needs copying to mapreduce cluster - looking for this:
        #<bioml>
        #  <note type="input" label="list path, taxonomy information">c:/Inetpub/wwwroot/ISB/data/mrtest.taxonomy.xml</note>
        #  <note type="input" label="protein, taxon">mydatabase</note>
        #  <note type="input" label="spectrum, path">c:/Inetpub/wwwroot/ISB/data/mrtest.123.mzXML</note>
        #  <note type="input" label="output, path">c:/Inetpub/wwwroot/ISB/data/mrtest.tandem</note>
        #  <note type="input" label="list path, default parameters">c:/Inetpub/wwwroot/ISB/data/parameters/isb_default_input_kscore.xml</note>    
        try:
            tree = ET.parse(mrh.cygwinify(xtandemParametersLocalPath))
        except Exception, inst:
           mrh.log( "Unexpected error opening X!Tandem parameters file %s: %s" % (xtandemParametersLocalPath, inst) )
        notes = tree.getiterator("note")
  
        for note in notes :
            if (note.get("label") != None) :
                if (note.attrib["label"] == "list path, taxonomy information" ) and ("" == taxonomyLocalPath) :
                    taxonomyLocalPath = mrh.my_abspath(note.text)
                    taxonomyName = mrh.S3CompatibleString(mrh.pathToTargetFileSystemPath(taxonomyLocalPath))
                    if mrh.runBangBang() : # set up to run as Ensemble Cloud Army job
                        newNoteText = '%s/%s' % ( jobDir , taxonomyName )
                        # this gets explicitly uploaded, but we need to tag it for download by ECA
                        mrh.setConfig("sharedFile_NoUpload_taxonomy%d" % nSharedFileIDs, newNoteText)
                        newNoteText = mrh.getConfig("sharedDir")+newNoteText
                        mrh.addFilenameTranslation(note.text, newNoteText) # so we can undo in local result
                        note.text = newNoteText
                        nSharedFileIDs = nSharedFileIDs+1
                    else :
                        note.text = taxonomyName
                elif (note.attrib["label"] == "list path, default parameters" ) :
                    xtandemParametersLocalPathList.extend([note.text]) # process this in next loop pass
                    defaultXtandemParametersName = mrh.S3CompatibleString(mrh.pathToTargetFileSystemPath(note.text))
                    if mrh.runBangBang() : # set up to run as Ensemble Cloud Army job
                        note.text =  mrh.tidypath('%s/%s/%s' % ( mrh.getConfig("sharedDir"),jobDir,defaultXtandemParametersName))
                    else :
                        note.text = defaultXtandemParametersName
                elif ((note.attrib["label"] == "spectrum, path" ) and ("" == spectrumName)) : 
                    # try to use gzipped copy instead to save upload time
                    note.text = mrh.attemptGZip(mrh.my_abspath( note.text ))
                    mrh.setConfig("sharedFile_spectrum%d" % nSharedFileIDs, note.text)
                    nSharedFileIDs = nSharedFileIDs+1
                    spectrumName = mrh.HadoopCacheFileName(note.text)
                    newnote = mrh.pathToTargetFileSystemPath(note.text)
                    if ( mrh.runBangBang() ) : # set up to run as Ensemble Cloud Army job 
                        newnote = mrh.tidypath(mrh.getConfig("sharedDir")+newnote)
                    mrh.addFilenameTranslation(note.text, newnote) # so we can undo in local result
                    note.text = newnote
                    cachefiles.append('%s#%s' % (spectrumName, note.text))
                elif ((note.attrib["label"] == "protein, taxon" ) and ("" == proteintaxon) ):
                    proteintaxon = note.text;
                    databaseRefs.extend([ note.text ]) # we'll look this up in the taxonomy file and make sure it gets up to S3
                elif (note.attrib["label"] == "output, path" ) and ("" == outputLocalPath) : # don't let default step on explicit
                    outputLocalPath = mrh.my_abspath(note.text)
                    outputS3CompatibleName = mrh.S3CompatibleString(mrh.pathToTargetFileSystemPath(outputLocalPath))
                    outputName = mrh.tidypath(mrh.getConfig("sharedDir")+outputS3CompatibleName)
                    mrh.addFilenameTranslation(note.text, outputName) # so we can undo in local result
                    note.text = outputName                        
                    mrh.setConfig("outputName",outputName)
                    mrh.setConfig("outputLocalPath",outputLocalPath)
                elif (note.attrib["label"] == "refine" ) and ("" == mrh.getConfig("refineSetting")) :
                    mrh.setConfig("refineSetting",note.text)
        xtandemParametersString = ET.tostring(tree.getroot()) # get entire XML doc as a string
        # now write the modified-for-S3/EMR tandem parameters to S3 jobdir        
        s3name = '%s/%s' % ( jobDir , xtandemParametersName )
        if ( not mrh.runLocal()) :
            mrh.set_contents_from_string(s3name, xtandemParametersString)
        if mrh.runBangBang() : # set up to run as Ensemble Cloud Army job
            mrh.setConfig("sharedFile_NoUpload_tandemParams%s"%paramsSuffix, s3name) # it's already up there, but ECA needs to download it
        paramsSuffix = "Default" # next pass is for handling default file
        

    # end nested param file handling
    if mrh.runBangBang() : # set up to run as Ensemble Cloud Army job
        # value="<ec2_filename>;<s3_filename>;<local_filename>"
        s3filename = mrh.tidypath("%s/%s"%(jobDir,outputS3CompatibleName))
        mrh.setConfig("resultFile_tandem%d"%nSharedFileIDs,"%s;%s;%s"%(outputName,s3filename,outputLocalPath)) # tell ECA to copy it back to S3 at end
        nSharedFileIDs = nSharedFileIDs+1

    # now examine the taxonomy file itself for database references - something like this
    # <?xml version="1.0"?>
    #  <bioml label="x! taxon-to-file matching list">
    #  <taxon label="mydatabase">
    #      <file format="peptide" URL="c:/Inetpub/wwwroot/ISB/data/dbase/yeast_orfs_all_REV.20060126.short.fasta" />
    #    </taxon>
    #  </bioml>
    #
    # make sure the referenced database is in S3 in the shared files area (that is, assume it gets
    # reused from job to job) and rewrite the S3 copy of the taxonomy file to point at that
    try:
        tree = ET.parse(taxonomyLocalPath)
    except Exception, inst:
       mrh.log( "Unexpected error opening X!Tandem taxonomy file %s: %s" % (taxonomyLocalPath, inst) )
    taxons = tree.getiterator("taxon")
    satisfiedDatabaseRefs = []
    for taxon in taxons :
        if ( taxon.attrib["label"] in databaseRefs ) :
            satisfiedDatabaseRefs.extend( [ taxon.attrib["label"] ] )
            dfiles = taxon.getiterator("file")
            for dfile in dfiles : # one or more database files
                localDatabaseName = mrh.my_abspath(dfile.attrib["URL"])
                mrh.setConfig("sharedFile_database%d" % nSharedFileIDs , localDatabaseName) # this causes file to upload to S3
                databaseName = mrh.HadoopCacheFileName(localDatabaseName)
                newURL = mrh.pathToTargetFileSystemPath(localDatabaseName)
                if ( mrh.runBangBang() ) : # set up to run as oddball Ensemble Cloud Army job
                    newURL = mrh.tidypath("/mnt/"+newURL)
                mrh.addFilenameTranslation(localDatabaseName, newURL) # so we can undo in local result
                dfile.attrib["URL"] = newURL
                cachefiles.append('%s#%s' % (databaseName, dfile.attrib["URL"]))
                nSharedFileIDs = nSharedFileIDs+1

    for ref in databaseRefs :
        if not ref in satisfiedDatabaseRefs :
            mrh.log('ERROR: could not find a reference to "%s" in taxonomy file %s.  Exiting with error.' %(ref,taxonomyLocalPath))
            exit(1)
            
    taxonomyString = ET.tostring(tree.getroot()) # get entire XML doc as a string
    if ( not mrh.runLocal()) :
        # now write the modified-for-S3/EMR taxonomy file to S3 jobdir        
        s3name = mrh.tidypath('%s/%s' % ( jobDir , taxonomyName ))
        mrh.set_contents_from_string(s3name,taxonomyString)

    xtandemParametersLocalPath = xtandemParametersLocalPathList[0]
    baseName = mrh.getConfig( "baseName" )
    mrh.setConfig("configNameJSON",'%s.cfg.json' % baseName)

    if (mrh.runHadoop()) : # see about getting tandem executable out to non-AWS cluster
        prev_tandem_url = tandem_url # avoid some redundant checks in multi-config setups
        tandem_url = mrh.getConfig("tandem_url")
        if (prev_tandem_url != tandem_url) : # avoid some redundant checks in multi-config setups
            if (tandem_url.startswith("http")) : # see if we need to get a local copy
                mrh.info("checking to see if we need to download %s" % tandem_url)
                mrh.runCommand("wget --progress=dot:mega -N "+tandem_url)
                tandem_file = tandem_url[tandem_url.rfind("/")+1:]
            else :
                tandem_file = tandem_url # local file
            mrh.setConfig("tandem_file",tandem_file)
            # HDFS cacheFile persists across jobs, and doesn't seem to notice when the file gets replaced
            # so we'll name the HDFS copy of tandem as tandem_<timestamp>
            timestamp = time.strftime("%Y%m%d%H%M%S",time.gmtime(os.path.getmtime(tandem_file)))
            mrh.uploadFile("tandem_file","",wantGZ=False,overwriteOK=True,filemode="a+x",decorate="_"+timestamp)
            tandem_link = mrh.getConfig("tandem_file")
        else :
            mrh.setConfig("tandem_file",tandem_link)
        xtandemCmd = tandem_link[tandem_link.rfind("/")+1:]
        cachefiles.extend(["hdfs://%s/%s#%s"%(mrh.getHadoopDir(),mrh.getConfig("tandem_file"),xtandemCmd)])
    else :
        xtandemCmd = "mrtandem"

    if ( mrh.runOldSkool() ) : # for debug and performance comparisons
        mrh.log("running in oldSkool mode - single node, single (possibly multithreaded) copy of tandem")
        mrh.setConfig( "numberOfClientNodes", "1" ) # single node
        mrh.setConfig( "numberOfMappers", "1" ) # single node
        nmappers = 1
        mrh.setConfig("numberOfTasksPerClient", "2" ) # one mapper, one reducer

    if ( not mrh.runLocal() and not mrh.runBangBang() ) :
        xtandemParametersCache = mrh.constructCacheFileReference( jobDir , mainXtandemParametersName)
        taxonomyCache = mrh.constructCacheFileReference( jobDir , taxonomyName )
        defaultparamsCache = mrh.constructCacheFileReference( jobDir , defaultXtandemParametersName )
        cachefiles.extend([ xtandemParametersCache, taxonomyCache, defaultparamsCache ])
        if (mrh.runAWS() and (1 == nParamFiles)) : # first time through on AWS EMR
            # create a job step to copy large data files from S3 to HDFS (you'd think that
            # you could just use cachefile mechanism from s3 but it doesn't seem to scale well)
            copierCommands = ""
            copierScript = '%s/copier-step.sh' % jobDirMain 
            copierScriptInputs = '%s/copier-step-inputs' % jobDirMain
            scripttext = "#!/bin/bash\nexec <&0\nwhile read line\ndo {\necho $line 1>&2 ; $line 1>&2\n}\n done\n"
            mrh.set_contents_from_string(copierScript,scripttext)
        
        # go through the config parameters, anything named "sharedFile_*" gets uploaded
        # to S3  - "aharedFile_GZ_*" uploads with a gzip preference
        for cfgKey,val in mrh.selectConfig().iteritems():
            if (cfgKey.startswith("sharedFile_") and not cfgKey.startswith("sharedFile_NoUpload_")):
                # do the upload to S3
                targetDir = ""
                mrh.uploadFile(cfgKey, targetDir, wantGZ=cfgKey.startswith("sharedFile_GZ_"),overwriteOK=("True"==mrh.getConfig("overwriteUploads","False"))) # side effect: after this call config speaks of data files in terms of S3
                if ( mrh.runAWS()) :
                    # and set up for copying S3 files out to shared files system (HDFS, or NFS for X!!Tandem)
                    # prepare a list of copy commands to be passed out to mappers
                    cmd = mrh.constructHadoopCacheFileCopyCommand(cfgKey)
                    if (-1 == copierCommands.find(cmd)) : # avoid redundant copy commands
                        copierCommands = copierCommands + cmd
        if (mrh.runAWS()) :
            if (len(configfiles) == nParamFiles) : # on the last or only paramfile
                mrh.set_contents_from_string(copierScriptInputs, copierCommands)
            nodecount = int(mrh.getConfig( "numberOfClientNodes","0" ))
            mapTasksPerClient = int(mrh.getConfig("numberOfTasksPerClient")) 
            if (0==nodecount) : # perhaps they specified mapper count instead
                nmappers = int(mrh.getConfig("numberOfMappers","0"))
                usedefault = False
                if (0==nmappers) :
                    mrh.setCoreConfig("numberOfMappers","8")
                    nmappers = int(mrh.getConfig("numberOfMappers"))
                    usedefault = True
                nodecount = nmappers/mapTasksPerClient
                if (0==nodecount) :
                    nodecount = 1
                if (usedefault) :
                    mrh.log("numberOfClientNodes not specified, using default %d" % nodecount)
                mrh.setCoreConfig( "numberOfClientNodes", str(nodecount))
            else :
                nmappers = (nodecount*mapTasksPerClient) 
        else :
            nmappers = int(mrh.getConfig( "numberOfMappers" ))
    else :
        nmappers = 1
    cachefilesStack.extend([cachefiles])
    
    # write parameters to for the record (after removing security info)
    mrh.scrubAndPreserveJobConfig( '%s/%s' % ( jobDir , mrh.getConfig("configNameJSON") ) )

    #
    # end for each config file
    #

# create the mapper1 input file
# there is only one reducer key
# each line of mapper input file tells mapper to take the nth of every m spectra
mapper_mult=4
# we output "mapper_mult" times as many pairs as we have mappers, so if anything goes wrong with one
# the others can level that out instead of somebody getting a double load
mapper1InputFile = '%s/mapper1-input-values' % jobDirMain
mapperInputs = ""
for count in range(nmappers*mapper_mult) :
    if (count > 0) :
        mapperInputs = mapperInputs + "\n"  # avoid a final newline - it causes an extra entry
    entry = '%5d %5d' % (count+1 , nmappers*mapper_mult) # want consistent line length so hadoop weights equally
    mapperInputs = mapperInputs + entry 
mrh.saveStringToFile(mapperInputs,mapper1InputFile)


# and now execute
# add one line for comment in github
nParamFiles = 0
bootstrapFile = ""
for xtandemParametersLocalPath in configfiles: # peruse each config file and do needed setup and file xfers
    worksteps = []
    mrh.selectConfig(nParamFiles)
    cachefiles = cachefilesStack[nParamFiles]
    nParamFiles = nParamFiles+1
    jobDir = mrh.getJobDir()
    baseName = mrh.getConfig( "baseName" )
    outputName = mrh.getConfig( "outputName", required=(not mrh.runLocal()) )
    mrh.info( "processing %s" % xtandemParametersLocalPath )
    
    if (mrh.runLocal()) :
        resultsFilename=mrh.getConfig("resultsFilename")
        mapperInputFile=mapper1InputFile
        reducerOutFile=""
        if (mrh.getDebug()) :
            xtandemCmd="/sashimi/trunk/trans_proteomic_pipeline/src/debug/tandem.exe"
        else :
            xtandemCmd="tandem"
        if ("yes"==mrh.getConfig("refineSetting")) :
            nsteps = 3
        else :
            nsteps = 2
        for step in range(1,nsteps+1) :
            mapper = '%s -mapper%d_%d /tmp %s ' % ( xtandemCmd, step, nParamFiles, xtandemParametersLocalPath)
            reducer = '%s -reducer%d_%d /tmp %s ' % ( xtandemCmd, step, nParamFiles, xtandemParametersLocalPath )
            reducerOutFile = mapperInputFile+".next"
            cmd = "cat " + mapperInputFile + " | " + mapper + " | sort | " + reducer + " > " + reducerOutFile
            mapperInputFile = reducerOutFile
            mrh.runCommand(cmd)
        wait = 1
        if (resultsFilename != "") :
            mrh.runCommand("cat "+reducerOutFile+" >> "+resultsFilename) # combine mapper and reducer logs
            mrh.runCommand("rm "+reducerOutFile) # delete mapper log
            cmd = "cat "+resultsFilename
            mrh.runCommand(cmd)

    elif mrh.runBangBang() :
        # running X!!Tandem MPI implementation
        pass
    else :
        if ( mrh.runAWS() and ("" == bootstrapFile) ) :
            # bootstrap actions to customize EMR image for our purposes - no need to run on master
            bootstrapFile = '%s/bootstrap.sh' % jobDirMain
           
            bootstrapText = '#!/bin/bash\nwget http://insilicos-public.s3.amazonaws.com/tandem_arch.sh ; sudo wget http://insilicos-public.s3.amazonaws.com/$(bash tandem_arch.sh)/mrtandem -O /usr/bin/mrtandem 2>&1 \n let n=$? \n sudo chmod 777 /usr/bin/mrtandem \n let n=n+$?\n mrtandem -mapreduceinstalltest\nlet n=n+$?\nexit $n\n'
            mrh.debug("writing AWS EMR bootstrap script to %s" % bootstrapFile)
            mrh.set_contents_from_string(bootstrapFile,bootstrapText)
            bootstrapActionInstallTandem = BootstrapAction("install mrtandem from insilicos-public.s3.amazonaws.com",'s3://elasticmapreduce/bootstrap-actions/run-if', ['instance.isMaster!=true','s3://%s/%s' % (bucketName, bootstrapFile)])

        mainXtandemParametersName = mrh.getConfig("mainXtandemParametersName")
        xtandemParameters = '%s/%s' % ( jobDir , mainXtandemParametersName )
        
        # write results here
        resultsDir = '%s/results' % jobDir
        if ( mrh.runAWS() ) :
            mrh.log("scripts, config and logs will be written to S3://%s/%s" % (mrh.bucketName,jobDir))
        else :
            mrh.log("scripts, config and logs will be written to %s" % (jobDir))

        # tell Hadoop to run just one reducer task, and set mapper task count in hopes of giving reducer equal resources
        if ( mrh.runAWS() ) :
            nodecount = int(mrh.getConfig( "numberOfClientNodes" ))
            mapTasksPerClient = int(mrh.getConfig("numberOfTasksPerClient")) 
            nmappers = (nodecount*mapTasksPerClient)
            timeoutMinutes = 45  # stuck jobs are expensive
            if (nmappers < 1) :
                nmappers = 1
        else : # straight up Hadoop
            nmappers = int(mrh.getConfig("numberOfMappers"))
            timeoutMinutes = 6000  # already own the cluster, might as well hang in there
            
        stepArgs = ['-jobconf','mapred.task.timeout=%d'%(timeoutMinutes*1000*60),'-jobconf','mapred.reduce.tasks=1','-jobconf','mapred.map.tasks=%d' % nmappers] # timeout in msec
        stepArgs.extend(['-jobconf','mapred.reduce.tasks.speculative.execution=false'])
        stepArgs.extend(['-jobconf','mapred.map.tasks.speculative.execution=false'])


        if ( mrh.runAWS() ) :
            xtandemArgs = 'hdfs %s' % mainXtandemParametersName  # passing xfer dir URL to EMR causes trouble
            if (1==nParamFiles) : # first time through
                # specify a streaming (stdio-oriented) step to copy database and spectra files from S3 to HDFS
                copierStep = boto.emr.StreamingStep( name = '%s-copyDataFromS3toHDFS' % jobName,
                                           mapper = '%s/%s' % ( baseURL , copierScript),
                                           reducer = 'NONE',
                                           input = '%s/%s' %  (baseURL, copierScriptInputs), 
                                           output = '%s/%s/copierStepResults' %  (baseURL, jobDirMain),
                                           step_args = ['-jobconf','mapred.task.timeout=2700000']) # 45 minute timeout for file transfer
                worksteps.extend([copierStep])

            # specify a streaming (stdio-oriented) step to run tandem (inherits stdin) then tidy up the result and copy to S3
            finalReportURL = '%s/%s/%s' %  (bucketName , jobDir, os.path.basename(outputName))
            finalOutputDir =  '%s/%s' %  (baseURL, resultsDir)
            finalOutputURLArg = ' -reportURLS3 %s' % finalReportURL # tells reducer where to put report, and implies final step
        else :
            xtandemArgs = 'hdfs://%s %s' % (mrh.getHadoopWorkDir(),mainXtandemParametersName) # explictly set xfer dir URL
            finalReportURL = 'hdfs://%s/%s' %  (mrh.getHadoopWorkDir(), os.path.basename(outputName))
            finalOutputDir =  'hdfs://%s/%s' %  (mrh.getHadoopDir(), resultsDir)
            finalOutputURLArg = ' -reportURL %s' % finalReportURL # tells reducer where to put report, and implies final step

        mrh.setConfig("finalReportURL",finalReportURL)


        if ( mrh.runOldSkool() ) : 
            # for debug and performance comparison, just runs regular tandem on a single node in the reducer
            
            # run tandem - "reducer99" is its cue to fall back into traditional single-node multi-thread behavior
            workStepOldSkool = boto.emr.StreamingStep( name = '%s-OldSkool-final' % baseName, # "-final" is cue to grab results below
                                       mapper = '%s -mapreduceinstalltest' % xtandemCmd,
                                       reducer = '%s -reducer99_%d %s %s -reportURL %s' % (xtandemCmd,nParamFiles,outputName, mainXtandemParametersName, finalReportURL),
                                       cache_files = cachefiles,
                                       input = '%s/%s' %  (baseURL, mapper1InputFile),
                                       output = '%s/%s' %  (baseURL, resultsDir),
                                       step_args = stepArgs)
            worksteps.extend([workStepOldSkool])

        else :
            conditionStepOutputDir = 'hdfs://'+mrh.getHadoopWorkDir()+'/output%d.1/'%nParamFiles
            workStep1 = boto.emr.StreamingStep( name = '%s-condition' % baseName,
                                           mapper = '%s -mapper1_%d %s' % (xtandemCmd, nParamFiles, xtandemArgs),
                                           reducer = '%s -reducer1_%d.%d %s' % (xtandemCmd, nParamFiles, nmappers, xtandemArgs),
                                           cache_files = cachefiles,
                                           input = '%s/%s' %  (baseURL, mapper1InputFile),
                                           output = conditionStepOutputDir,                                  
                                           step_args = stepArgs)

            if ( mrh.getConfig("refineSetting") == "yes" ) :
                # we need another step to do refining
                processStepOutputDir = 'hdfs://'+mrh.getHadoopWorkDir()+'/output%d.2/'%nParamFiles
                reducer2_URL_arg = ""
                reducer3_URL_arg = finalOutputURLArg
                processStepSuffix = ''
            else :
                # no refinement, can stop at reducer 2
                processStepOutputDir = finalOutputDir
                reducer2_URL_arg = finalOutputURLArg
                processStepSuffix = '-final' # helps with non-EMR hadoop multi-param runs
                
            processStepCacheFiles = list(cachefiles) # make a copy of the list
            processStepCacheFiles.append('hdfs://%s/reducer1_%d#reducer1_%d'%(mrh.getHadoopWorkDir(),nParamFiles,nParamFiles))
            workStep2 = boto.emr.StreamingStep( name = '%s-process%s' % (baseName,processStepSuffix),
                                           mapper = '%s -mapper2_%d %s' % (xtandemCmd, nParamFiles, xtandemArgs),
                                           reducer = '%s -reducer2_%d.%d %s %s' % (xtandemCmd, nParamFiles, nmappers, xtandemArgs, reducer2_URL_arg),
                                           cache_files = processStepCacheFiles,
                                           input = 'hdfs://'+mrh.getHadoopWorkDir()+'/output%d.1/'%nParamFiles,
                                           output = processStepOutputDir,                            
                                           step_args = stepArgs)
          
            if ( "yes" == mrh.getConfig("refineSetting") ) :
                refineStepCacheFiles = list(cachefiles) # make a copy of the list
                refineStepCacheFiles.append('hdfs://%s/reducer2_%d#reducer2_%d'%(mrh.getHadoopWorkDir(),nParamFiles,nParamFiles))
                workStep3 = boto.emr.StreamingStep( name = '%s-refine-final' % baseName,
                                           mapper = '%s -mapper3_%d %s' % (xtandemCmd, nParamFiles, xtandemArgs),
                                           reducer = '%s -reducer3_%d %s %s' % (xtandemCmd, nParamFiles, xtandemArgs, reducer3_URL_arg),
                                           cache_files = refineStepCacheFiles,
                                           input = 'hdfs://'+mrh.getHadoopWorkDir()+'/output%d.2/'%nParamFiles,
                                           output = finalOutputDir,                                  
                                           step_args = stepArgs)
                worksteps.extend([workStep1,workStep2,workStep3])
            else :
                worksteps.extend([workStep1,workStep2])

        workstepsStack.extend( [worksteps] )
        mrh.setConfig("finalOutputDir",finalOutputDir)
        mrh.setConfig("resultsDir",resultsDir)
        mrh.setConfig("completed",False)
        
    # end for each paramfile
    
if mrh.runBangBang() :
    # running X!!Tandem MPI implementation
    # fire up a cluster using our Ensemble Cloud Army AMIs
    mrh.tidyConfigStack() # multi-job? move all common settings to core config
    cloudArmyDir = mrh.tidypath(os.path.abspath(mrh.getConfig("cloudArmyDir",syspath+"/../ensemble/src"))+os.sep)
    nodecount = int(mrh.getConfig( "numberOfClientNodes","-1" ))
    if (nodecount == -1) : # unspecified
        mapTasksPerClient = int(mrh.getConfig("numberOfTasksPerClient")) 
        nmappers = int(mrh.getConfig("numberOfMappers","0"))
        usedefault = False
        if (0==nmappers) :
            mrh.setCoreConfig("numberOfMappers","8")
            nmappers = int(mrh.getConfig("numberOfMappers"))
            usedefault = True
        nodecount = nmappers/mapTasksPerClient
        if (0==nodecount) :
            nodecount = 1
        if (usedefault) :
            mrh.log("numberOfClientNodes not specified, using default %d" % nodecount)

            
    if ( nodecount < 1) :
        nodecount = 1  # we consider the head node in the count, for EMR comparison
    mrh.setCoreConfig( "numberOfClientNodes", "%d"%(nodecount-1) ) # head node acts as a client in bangbang
    mrh.setCoreConfig("RMPI_FrameworkScriptFileName","%sbangbang.R"%cloudArmyDir)
    mrh.setCoreConfig("frameworkSupportScript","%seca_common_framework.R"%cloudArmyDir)
    mrh.setCoreConfig("scriptFileName",mrh.getConfig("frameworkSupportScript")) # just to keep ECA happy
    resultdir = mrh.getCoreJobDir()
    cmd = 'python %seca_launch_rmpi.py %s %s' % (cloudArmyDir, mrh.getCoreConfigAsTmpfile(resultdir), mrh.getConfigAsTmpfile(resultdir) )
    mrh.log("run ECA-RMPI...") 
    mrh.runCommand( cmd )
elif (mrh.runAWS()) :
    keepalive = ("True"==mrh.getConfig("keepHead","False"))
    if ( keepalive ) :
        failure_action = 'CANCEL_AND_WAIT'
    else :
        failure_action = 'TERMINATE_JOB_FLOW'

    # are we planning a spot bid instead of demand instances?
    spotBid = mrh.getConfig("spotBid","")
    conn = mrh.conn

    if ("" != spotBid) :
        from boto.emr.instance_group import InstanceGroup  # spot EMR is post-2.0 stuff - 2.1rc2 is known to work
        if ('%' in spotBid) : # a percentage, eg "25%" or "25%%"
            spotBid = mrh.calculateSpotBidAsPercentage( spotBid, mrh.getConfig("ec2_client_instance_type"), 0.20 ) # about 20% more for EMR instances
        launchgroup = "MRT"+mrh.getConfig( "baseName" ) +"_"+mrh.getConfig("jobTimeStamp")
        mrh.setCoreConfig("launchgroup",launchgroup)
        instanceGroups = [  
            InstanceGroup(1, 'MASTER', mrh.getConfig("ec2_head_instance_type"), 'SPOT', 'master-%s' % launchgroup, spotBid),  
            InstanceGroup(int(mrh.getConfig("numberOfClientNodes")), 'CORE', mrh.getConfig("ec2_client_instance_type"), 'SPOT', 'core-%s' % launchgroup, spotBid)  
            ]
        jf_id = conn.run_jobflow(name = baseName,
                            log_uri='s3://%s/%s' %  (bucketName, jobDirMain),
                            hadoop_version="0.20",
                            ec2_keyname=mrh.getConfig( "RSAKeyName", required=False ),
                            action_on_failure=failure_action,
                            keep_alive=keepalive,
                            instance_groups=instanceGroups,
                            enable_debugging=("False"==mrh.getConfig("noDebugEMR","False")),
                            steps=[workstepsStack[0][0]],
                            bootstrap_actions=[bootstrapActionInstallTandem])
    else :
        mrh.log("Using demand instances.  Consider using spotBid parameter for less expensive operation.")
        jf_id = conn.run_jobflow(name=jobName,
                            log_uri='s3://%s/%s' %  (bucketName, jobDirMain),
                            hadoop_version="0.20",
                            ec2_keyname=mrh.getConfig( "RSAKeyName", required=False ),
                            action_on_failure=failure_action,
                            keep_alive=keepalive,
                            master_instance_type=mrh.getConfig("ec2_head_instance_type"),
                            slave_instance_type=mrh.getConfig("ec2_client_instance_type"),
                            enable_debugging=("False"==mrh.getConfig("noDebugEMR","False")),
                            num_instances=(int(mrh.getConfig("numberOfClientNodes"))+1), # +1 for master
                            steps=[workstepsStack[0][0]],
                            bootstrap_actions=[bootstrapActionInstallTandem])
    # multi-config, multi-step jobs may overwhelm a single boto action
    for n in range(1,len(workstepsStack[0])) :
        conn.add_jobflow_steps(jf_id,[workstepsStack[0][n]]) # so add a step at a time
    for m in range(1,len(workstepsStack)) : 
        for n in range(0,len(workstepsStack[m])) : 
            conn.add_jobflow_steps(jf_id,[workstepsStack[m][n]]) # so add a step at a time

    wait = 30 # much less than 10 sec and AWS gets irritated and throttles you back
    lastState = ""
    while True:
      jf = conn.describe_jobflow(jf_id)
      if (lastState != jf.state) : # state change
          mrh.log("cluster status: "+jf.state)
          lastState = jf.state
      else :
          mrh.log_progress() # just put a dot

      for nConfig in range(len(configfiles)) :
        mrh.selectConfig(nConfig)
        if (not mrh.getConfig("completed")) :
            finalOutputDir=mrh.getConfig("finalOutputDir")
            resultsDir=mrh.getConfig("resultsDir")
            jobDir=mrh.getJobDir()
            outputName=mrh.getConfig("outputName")
            outputLocalPath=mrh.getConfig("outputLocalPath")
            # grab the results - this is the stdout and stderr of the final reducer step of current search
            concat = ""
            mask = '%s/part-' % resultsDir
            for part in BucketListResultSet(mrh.s3bucket, prefix=mask) :
                # all results in one string
                concat = concat + mrh.get_contents_as_string(part)
            if (len(concat) > 0) :
                mrh.log("Done.  X!Tandem logs:")
                # write to file?
                resultsFilename=mrh.getConfig("resultsFilename")
                if (resultsFilename != "") :
                    f = open(resultsFilename,"a")
                    f.write(concat)
                    f.close()
                    mrh.log('(this log is also written to %s)' % resultsFilename)
                mrh.log(concat)   
                # and of course grab the tandem result file
                s3name = '%s/%s'%(jobDir, os.path.basename(outputName))
                mrh.get_contents_to_filename(s3name,outputLocalPath)
                mrh.log('Results written to %s' % outputLocalPath)
                mrh.setConfig("completed",True)
                lastState = '' # just to provoke reprint of state on console
           
      if lastState == 'COMPLETED':
        break
      if lastState == 'FAILED':
        break
      if lastState == 'TERMINATED':
        break
      sleep(wait)
elif (not mrh.runLocal()) : # non-EMR hadoop
    mrh.log("begin execution")
    for nConfigs in range(0,len(workstepsStack)) :
        mrh.selectConfig(nConfigs)
        for workstep in workstepsStack[nConfigs] :
            mrh.doHadoopStep(workstep)
        # nab the tandem output
        max_retry = 4
        for retry in range(max_retry+1) :
            try :
                results = mrh.get_contents_as_string('%s/%s/part-00000' % (mrh.getHadoopDir(),mrh.getConfig("resultsDir")))
                resultsFilename = mrh.getConfig("resultsFilename")
                if (resultsFilename != "") :
                    f = open(resultsFilename,"a") # open for append
                    f.write(results)
                    f.close()
                # and of course grab the tandem result file
                mrh.get_contents_to_filename(mrh.getConfig("finalReportURL"),mrh.getConfig("outputLocalPath"))
                # also place a copy in results dir
                if (resultsFilename != "") :
                    shutil.copy(mrh.getConfig("outputLocalPath"),os.path.dirname(resultsFilename)+"/"+os.path.basename(mrh.getConfig("outputLocalPath")))
                # and tidy up
                mrh.removeRemoteFile(mrh.getHadoopWorkDir()+"/reducer1*") # kill our tempfiles
                mrh.removeRemoteFile(mrh.getHadoopWorkDir()+"/mapper2*") # kill our tempfiles
                mrh.removeRemoteFile(mrh.getHadoopWorkDir()+"/reducer2*") # kill our tempfiles
                mrh.removeRemoteFile(mrh.getHadoopWorkDir()+"/mapper3*") # kill our tempfiles
                
                mrh.log("Done.  X!Tandem logs:")
                mrh.log(results)
                if (resultsFilename != "") :
                    mrh.log('(this log is also written to %s)' % resultsFilename)        
                mrh.log('Results written to %s' % mrh.getConfig("outputLocalPath"))
                break
            except Exception, exception:
                pass
            if (max_retry == retry) :
                mrh.log( exception )
            else :
                sleep(10)                
                
                                 
    # todo - monitor job progress
# all configs processed
mrh.log_close()
