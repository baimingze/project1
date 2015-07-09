#!/opt/local/bin/python

# script for launching ensemble learning jobs in Amazon EC2 using RMPI
#
# The idea is to launch a bunch of StarCluster images and configure them
# on the fly.  Not all StarCluster images support the userdata-as-script
# mechanism so we connect with SSH after boot and push our bootstrap up
# that way instead, which is more StarCluster-y anyway.

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
import boto.ec2
import boto.s3
from boto.ec2.regioninfo import RegionInfo
from boto.s3.key import Key
from time import sleep
import datetime 

# fancy stuff for quiet use of older paramiko+pycrypto on win64
import warnings
with warnings.catch_warnings():
	warnings.filterwarnings("ignore",category=DeprecationWarning)
	import paramiko # python ssh implementation

import eca_launch_helper as eca # functions common to RMPI and MapReduce versions

# code for firing up nodes and configuring them in parallel processes
import multiprocessing
scriptsFileName = "/root/s.tar.gz"
scriptsFileB64Name = "/root/s.b64"
MSG_SUCCESS = "success"
MSG_FAILURE = "failed"
wait = 10 # much less than this and AWS gets irritated and throttles you back
class startNode(multiprocessing.Process):
	def __init__ (self,nodenum,verbose,ip_address,privkey,scriptsFileB64Text,bootstrap,readConn, writeConn, lock):
		multiprocessing.Process.__init__(self) # base class
		self.verbose = verbose
		self.nodenum = nodenum
		self.ip_address = ip_address
		self.privkey = privkey
		self.scriptsFileB64Text = scriptsFileB64Text
		self.bootstrap = bootstrap
		self.success = False
		self.retries = 0
		# pipes for reporting status to parent process
		self.readConn = readConn
		self.writeConn = writeConn
		# avoid log-blasts
		self.last_logmsg=""
		self.logmsg_repeats=0
	def log(self,msg,verbose):
		if (verbose) :
			if (msg != self.last_logmsg) :
				if ( self.logmsg_repeats > 0 ) :
					self.writeConn.send("node %d: %s (message repeats %d times)"%(self.nodenum,self.last_logmsg,self.logmsg_repeats))
					self.logmsg_repeats = 0
				self.writeConn.send("node %d: %s"%(self.nodenum,msg))
				self.last_logmsg = msg
			else :
				self.logmsg_repeats=self.logmsg_repeats+1
				if ( self.logmsg_repeats > 9 ) :
					self.writeConn.send("node %d: %s (message repeats %d times)"%(self.nodenum,self.last_logmsg,self.logmsg_repeats))
					self.logmsg_repeats = 0
	def run(self):
		halfBaked = False # for detecting partial completion
		while ( (not halfBaked) and (not self.success) )  :
			import StringIO
			self.ssh = paramiko.SSHClient()
			self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
			halfBaked = False # for detecting partial completion
			try:
				self.ssh.connect(self.ip_address, username='root', pkey=self.privkey)
				content = StringIO.StringIO()
				content.write(self.bootstrap)
				content.seek(0)
				# write our bootstrap script
				line = content.readline()
				self.log("installing userdata and boot script via ssh...",self.verbose)
				self.ssh.exec_command("rm -f bootstrap.py")
				while line :
					stdin, stdout, stderr = self.ssh.exec_command("echo '%s' >> bootstrap.py"%line)
					o = stdout.readlines()
					e = stderr.readlines()
					if (0 < len(o)) :
						self.log(o,self.verbose)
					if (0 < len(e)) :
						self.log(e,True)
					line = content.readline()
				# write our config scripts, in chunks
				b64content = StringIO.StringIO()
				b64content.write(self.scriptsFileB64Text)
				b64content.seek(0)
				line = b64content.read(1024)
				self.ssh.exec_command("rm -f %s"%scriptsFileB64Name)
				while line  :
					stdin, stdout, stderr = self.ssh.exec_command("echo -n '%s' >> %s"%(line,scriptsFileB64Name))
					o = stdout.readlines()
					e = stderr.readlines()
					if (0 < len(o)) :
						self.log(o,self.verbose)
					if (0 < len(e)) :
						self.log(e,True)
					line = b64content.read(1024)
				self.log("starting configuration script...",self.verbose)
				halfBaked = True # clear this once we've done all steps
				stdin, stdout, stderr = self.ssh.exec_command('chmod 777 bootstrap.py')
				stdin, stdout, stderr = self.ssh.exec_command('python bootstrap.py &')
				if (0 < len(o)) :
					self.log(o,self.verbose)
				if (0 < len(e)) :
					self.log(e,True)
				self.log("boot script is running.",self.verbose)
				halfBaked = False # full completion
				self.success = True
			except Exception, inst :
				if (self.retries*wait > 120) : # don't start barking right away
					self.log(str(inst),True)
				sleep(wait)
				self.retries = self.retries+1
			self.ssh.close()
		# report state via pipe
		if (self.success) :
			self.writeConn.send(MSG_SUCCESS)
		else :
			self.writeConn.send(MSG_FAILURE)

#
# main code
#
if __name__ == '__main__':  # needed for multiprocessing under windows

	eca.loadConfig("RMPI") # get config as directed by commandline, RMPI style

	coreJobDir = eca.getCoreJobDir()

	# pass a copy of the RMPI helper script to head node
	eca.setCoreConfig("RMPI_FrameworkScriptFileName",eca.tidypath(eca.getConfig("RMPI_FrameworkScriptFileName",os.path.dirname(sys.argv[0])+"/eca_rmpi_framework.R")))
	eca.setCoreConfig("frameworkSupportScript",eca.tidypath(eca.getConfig("frameworkSupportScript",os.path.dirname(sys.argv[0])+"/eca_common_framework.R")))

	# scan the user script for any packages that may need to be installed on target systems
	# and create a script to do the installation
	packageInstallerScriptText = eca.create_R_package_loader_script(eca.getConfig("scriptFileName"))

	if ( eca.runLocal() ) :
		resultsFilenameList = ""
		returncode = 0 # until shown otherwise
		for n in range(0,len(eca.cfgStack)) :
			eca.selectConfig(n)
			# write parameters to jobDir for the record (after removing security info)
			# note extension on filename dictates output format - .r or .json
			configfile = '%s/%s.cfg.r' % ( coreJobDir, eca.getConfig("eca_uniqueName") )
			eca.scrubAndPreserveJobConfig( configfile, n )
			rScriptCmd = "Rscript"
			if ("win32" == sys.platform) :  # find R installation via registry
				from _winreg import *
				try:
					rguicmd = QueryValue(HKEY_CLASSES_ROOT,r'RWorkspace\shell\open\command')
					# something like '"C:\Program Files\R\R-2.8.1\bin\RGui.exe" "%1"'
					rguicmd = rguicmd[0:rguicmd.find('" "')+1]
					cmd = rguicmd[0:rguicmd.rfind('\\')+1]+'Rscript.exe"'
					rScriptCmd = cmd
				except:
					pass # hope they have path set up
			# execute the package installer script
			eca.setCoreConfig("packageInstaller", '%s/%s.installpackages.r' % ( coreJobDir, eca.getConfig("eca_uniqueName") ))
			eca.saveStringToFile(packageInstallerScriptText, eca.getConfig("packageInstaller"))
			cmd = rScriptCmd + " " + eca.getConfig("packageInstaller")
			eca.log( "run: " + cmd )
			os.system(cmd)
			# now run the job
			cmd = rScriptCmd + " " + eca.getConfig( "RMPI_FrameworkScriptFileName" ) + " " + configfile        
			resultsFilename = eca.getConfig( "resultsFilename","" )
			if ( resultsFilename != "" ) :
				cmd = cmd + " > " + resultsFilename+".tmp"
			eca.log( "run: " + cmd )
			result = os.system( cmd )
			if ( 0 != result ) :
				eca.log("command failed")
				returncode = returncode + result
			elif ( resultsFilename != "" ) :
				eca.log( "save results to %s:" % resultsFilename )
				os.system("cat "+resultsFilename+".tmp")
				os.system("cat "+resultsFilename+".tmp >> " + resultsFilename)
				os.system("rm "+resultsFilename+".tmp")
				nextname = "\n "+resultsFilename+" "
				if not nextname in resultsFilenameList :
					resultsFilenameList = resultsFilenameList + nextname
		if (len(eca.cfgStack) > 1) and (resultsFilenameList != "") : # offer a review of where to find results
			eca.log("All jobs complete.  Individual results in:"+resultsFilenameList)
			
		sys.exit( returncode )

	rsaFileName=eca.getConfig("RSAKeyFileName")
	rsaKeyName=eca.getConfig("RSAKeyName")
	rsaFile=open(rsaFileName,"r")
	rsaText=rsaFile.read()

	# replace the rsa key with the actual contents
	eca.setCoreConfig("RSAKey", rsaText)
	# set the mode for the node we'll instantiate:
	eca.setCoreConfig("mode","head")

	# declare where master and clients should place local copy of data and script from S3, if not already set
	eca.setCoreConfig("dataDir",eca.getConfig("dataDir","/mnt/"))

	aws_access_key_id = eca.getConfig("aws_access_key_id")
	aws_secret_access_key = eca.getConfig("aws_secret_access_key")
	ami_head_id = eca.getConfig("ami_head_id")

	aws_region = eca.getConfig("aws_region")

	if ( eca.getConfig("ec2_endpoint","") != "" ) :
		eca.info("using "+eca.getConfig("ec2_endpoint"))
		aws_region = boto.ec2.connection.RegionInfo(name=eca.getConfig("aws_region"),endpoint=eca.getConfig("ec2_endpoint"))
		conn = aws_region.connect(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
	else :
		conn = boto.ec2.connect_to_region(aws_region,aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

	# check for security group and only create if not exists

	secGroupList = conn.get_all_security_groups()
	foundSSHSecGroup = False

	for secGroup in secGroupList:
		if secGroup.name == "ECA-SSH":
			foundSSHSecGroup = True
			sshSecurityGroup = secGroup

			# make sure this is the rule that we expect
			secRules = sshSecurityGroup.rules
			if (len(secRules) != 1):
				eca.log("ECA-SSH group found, but wrong number of rules\n")
				exit(-1)

			secRule = secRules[0]

			# verify that we only have one CIDR grant here
			if (len(secRule.grants) != 1):
				eca.log("unexpected number of CIDR grants\n")
				exit(-1)
			
			if (not (secRule.from_port == 22) and (secRule.to_port == 22) and (secRule.ip_protocol == "tcp") and (secRule.grants[0].cidr_ip == u'0.0.0.0/0')):
				eca.log("ECA-SSH group found, but rule is wrong\n")
				exit(-1)
			
			
			eca.debug("ssh security group (ECA-SSH) found and is valid")
			break

	if (not foundSSHSecGroup):
		eca.log('creating SSH security group on EC2')
		sshSecurityGroup = conn.create_security_group('ECA-SSH', 'ssh')
		sshSecurityGroup.authorize('tcp', 22, 22, '0.0.0.0/0')
		eca.log("SSH security group (ECA-SSH) created")

	#
	# verify S3 bucket and files
	# we're assuming that there's always a local copy of the files.
	#

	# create a directory on S3 for this job named as combo of job_config and timestamp
	# we will always place a copy of the script there, and the results will go there
	# data sets we don't copy over every time - they're big and don't change usually
	coreJobDir = eca.S3CompatibleString(eca.getCoreJobDir())

	eca.debug("beginning checks of files in S3")

	# pass our latest and greatest EC2 node startup code
	ciDir = "%s/cluster_infrastructure"%os.path.dirname(sys.argv[0])
	eca.setCoreConfig("start_node.py","%s/start_node.py"%ciDir)

	# now check the data files for existance and match, and put up a local copy of the script
	# configParamName-for-file, config-ID, target-dir-for-file, want-GZ
	# side effect: value for config[<key>] gets set to its S3 equivalent

	s3FileList = [ ("RMPI_FrameworkScriptFileName", -1, coreJobDir, False),
			   ("frameworkSupportScript", -1, coreJobDir, False),
			   ("scriptFileName", -1, coreJobDir, False),
			   ("start_node.py", -1, coreJobDir, False)] 

	# go through the config parameters, anything named "sharedFile_*" gets uploaded
	# to S3 - sharedFile_GZ_* gets uploaded with a gzip preference
	for n in range(-1,len(eca.cfgStack)) :
		for cfgKey,val in eca.selectConfig(n).iteritems():
			if (cfgKey.startswith("sharedFile_") and not cfgKey.startswith("sharedFile_NoUpload_")) :
				fullLocalPath = eca.my_abspath( val )              # convert relative path to absolute
				eca.setConfig( cfgKey, fullLocalPath)
				s3FileList.append( ( cfgKey, n, "", cfgKey.startswith("sharedFile_GZ_") ) )
				
	# and upload
	eca.uploadToS3( s3FileList )       

	# tell head node where to leave results
	localLogFileName = eca.getCoreJobDir()+"/eca_head.log"
	eca.setCoreConfig("logFileName", eca.S3CompatibleString(localLogFileName))
	eca.setCoreConfig("s3JobDir", coreJobDir)
	for n in range(0,len(eca.cfgStack)) :
		eca.selectConfig(n)
		eca.setConfig("reportFileName", eca.getJobDir()+"/rmpi_computation.log")

	# silly to keep clients without head
	if ("True"==eca.getConfig("keepClients","False")) :
		eca.setCoreConfig("keepHead","True")  

	# transmit the script for updating R packages as needed
	eca.setCoreConfig("packageInstaller", '%s/%s.installpackages.r' % ( coreJobDir, eca.getConfig("eca_uniqueName") ))
	eca.saveStringToFile(packageInstallerScriptText, eca.getConfig("packageInstaller"))

	# are we planning a spot bid instead of demand instances?
	spotBid = eca.getConfig("spotBid","")
	if ("" != spotBid) :
		launchgroup = "ECA"+eca.getConfig( "baseName" ) +"_"+eca.getConfig("jobTimeStamp")
	else :
		launchgroup = ""
	eca.setCoreConfig("launchgroup",launchgroup)

	# how many nodes to launch?
	numberOfNodes = int(eca.getConfig("numberOfNodes","0")) # modern way to describe cluster size

	if ( 0 == numberOfNodes ) : # legacy config file?
		clientcount = int(eca.getConfig("numberOfClientNodes","0")) # we used to specify client count
	else :
		clientcount = numberOfNodes - 1
	eca.setCoreConfig("numberOfClientNodes",str(clientcount))

	# watch for single node, single core case
	mpiSlotsPerClient = int(eca.getConfig("numberOfRTasksPerClient"))
	mpiSlotsOnHead = int(eca.getConfig("numberOfRTasksOnHead"))
	if ( 0 == (mpiSlotsOnHead+(clientcount * mpiSlotsPerClient) ) ) :
		eca.setCoreConfig("numberOfRTasksOnHead","2")

	# prepare to pass our config scripts and config data
	scriptsFileB64Text = eca.files_as_tar_gz_base64_string([
						['%s/config_nfs_head.sh'%ciDir,''],
						['%s/config_nfs_client.sh'%ciDir,''],
						['%s/sshtest.sh'%ciDir,''],
						['%s/config_node_for_ica.sh'%ciDir,''],
						['%s/start_node.sh'%ciDir,''],
						['%s/setup.py'%ciDir,''],
						['%s/start_node.py'%ciDir,''],
						['userdata.json',eca.getConfigToString()]
						])

	# prep the runtime bootstrap script
	# this serves to bootstrap a StarCluster AMI for ICA use and convey the job parameters
	bootstrap  = "#!/usr/bin/python\n" 
	bootstrap += 'import os, stat, base64, subprocess\n'
	bootstrap += 'def log_cmd(cmd) :\n'
	bootstrap += '	subprocess.call("echo \\\"%s\\\" | tee -a /var/log/eca_config.log"%cmd,shell=True)\n'
	bootstrap += '	subprocess.call("%s 2>&1 | tee -a /var/log/eca_config.log"%cmd,shell=True)\n'
	# start of actions
	bootstrap += 'ud = "/mnt/userdata"\n'
	bootstrap += 'if not os.path.exists(ud):\n'
	bootstrap += '	os.makedirs(ud)\n'
	bootstrap += 'else:\n'
	bootstrap += '	print "boot script already ran"\n' # we've already done this 
	bootstrap += '	quit()\n'
	bootstrap += 'log_cmd("chmod -R 777 "+ud)\n'
	bootstrap += 'fn="%s"\n'%scriptsFileName
	bootstrap += 'f = open(fn,"w")\n'
	bootstrap += 'f.write(base64.b64decode(open("%s").read()))\n' % scriptsFileB64Name
	bootstrap += 'f.close()\n'
	bootstrap += 'os.chmod(fn,stat.S_IEXEC|stat.S_IREAD|stat.S_IWRITE)\n'
	bootstrap += 'log_cmd("tar -xvzf %s"%fn)\n'
	bootstrap += 'log_cmd("mv userdata.json %s "%ud)\n'
	bootstrap += 'log_cmd("touch /mnt/eca-gotuserdata")\n'
	bootstrap += 'log_cmd("/root/start_node.sh")\n'
	bootstrap += 'log_cmd("exit 0")\n'

	# write parameters to S3 for the record (after removing security info)
	eca.scrubAndPreserveJobConfig('%s/config.json' % ( coreJobDir ))

	if ("True"==eca.getConfig("is_cci","False")) :
		placement_group_name = eca.getConfig("placementGroup","ECA")
		needsgroup = False
		try : # possibly throws an exception if group not present
			needsgroup = (0 == len(conn.get_all_placement_groups([placement_group_name]))) 
		except Exception, inst:
			needsgroup = True
		if ( needsgroup ) :
			eca.log('creating EC2 placement group %s'%placement_group_name)
			conn.create_placement_group(placement_group_name)
	else :
		placement_group_name = None

	eca.log('starting cluster with 1 head and %d client nodes (this may take a few minutes)'%clientcount)

	head_instance_type = eca.getConfig("ec2_head_instance_type")
	if ("" != eca.getConfig("aws_placement","")) :
		# use explicit availability zone
		aws_placement = eca.getConfig("aws_region")+getConfig("aws_placement")
	else :
		# let AWS choose the least busy availability zone
		aws_placement = None

	# launch head and clients all at the same time
	global instances
	if ( spotBid != "" ) :
		if ('%' in spotBid) : # a percentage, eg "25%" or "25%%"
			spotBid = eca.calculateSpotBidAsPercentage( spotBid, head_instance_type )
			# specify that they all launch together, and in the same availability zone (without necessarily saying which one)
			reservation = conn.request_spot_instances(spotBid, ami_head_id, count=clientcount+1, type='one-time', valid_from=None,
						valid_until=None, launch_group=eca.getConfig("launchgroup"), availability_zone_group=eca.getConfig("launchgroup"),
						key_name=rsaKeyName, security_groups=['default', 'ECA-SSH'], user_data=None, addressing_type=None,
						instance_type=head_instance_type, placement=aws_placement, kernel_id=None, ramdisk_id=None, monitoring_enabled=False)
			# reservation is a list of "count" spot requests
			instances=[] 
			for spotreq in reservation : 
				spotreq_id = spotreq.id
				lastState = ""
				failed = False
				while not failed :
					spotreq = conn.get_all_spot_instance_requests([spotreq_id])[0] # update
					if (spotreq.state == lastState) :
						eca.log_progress() # just put a dot
						sleep(wait)
					else :
						# state change
						eca.log_no_newline( 'spot request %s with bid=%s status: %s' % (spotreq_id,spotBid,spotreq.state) ) 
						lastState = spotreq.state
					if spotreq.state == u'active' :
						res = conn.get_all_instances([spotreq.instance_id])[0]
						instances.append(res.instances[0])
						break
					elif spotreq.state == u'failed' :
						eca.log('unknown spot request startup failure')
						failed = true
						break
					elif spotreq.state == u'cancelled' :
						eca.log('spot request startup cancelled')
						failed = true
						break
				if failed :
					break
	else :
		eca.log("note: using demand instances, consider using spotBid parameter to reduce costs")
		reservation = conn.run_instances(ami_head_id,min_count=clientcount+1, max_count=clientcount+1, security_groups=['default', 'ECA-SSH'], key_name=rsaKeyName, user_data=None, instance_type=head_instance_type, instance_initiated_shutdown_behavior="terminate",placement_group=placement_group_name, monitoring_enabled=False)
		instances = reservation.instances


	# now start a bunch of threads to config the nodes all at once
	n_succeeded=0
	booters=[]
	# use ssh to run startup script on each node (ec2 userdata boot scripts are
	# a good way to do this, but not all StarCluster AMIs have that, and anyway that
	# mechanism imposes a 16KB file size limit)
	wait = 10 # much less than this and AWS gets irritated and throttles you back
	if ( not os.path.isfile( eca.getConfig("RSAKeyFileName") ) ):
		eca.log("Need the RSAKeyFileName parameter to give location of keypair file for ssh use")
		eca.log("terminating all nodes and quitting")
		for i in instances :
			i.terminate()
		quit()
	lastState = []
	statuses=[] # for building a single-line representation of cluster status
	for i in range(len(instances)) :
		lastState.append("")
		statuses.append("?")
	nRunning = 0
	privkey = paramiko.RSAKey.from_private_key_file(eca.getConfig("RSAKeyFileName"))
	lock = multiprocessing.Lock()
	last_status_string=""
	eca.log("status, nodes 0-%d: P=pending B=booting R=running T=terminated"%(len(instances)-1))
	while True :
		for i in range(len(instances)) :
			instance = instances[i]
			try:
				instance.update()
			except:
				pass # probably an unlucky early "instance does not exist" error
			if (instance.state == u'pending') :
				statuses[i] = "P"
			elif (instance.state == u'running') :
				if ( statuses[i] != "R" ) :
					statuses[i] = "B" # still booting 
			elif (instance.state == u'terminated') :
				statuses[i] = "T"
			else :
				statuses[i] = "?"
			if (instance.state != lastState[i]) :
				if (instance.state == u'running') :
					# start a new process to configure this node
					readConn, writeConn = multiprocessing.Pipe()
					booter = startNode(i,eca.verbose(),instances[i].ip_address,privkey,scriptsFileB64Text,bootstrap,readConn, writeConn, lock)
					booter.start()
					booters.append(booter)
			if instance.state == u'terminated' :
				eca.log('unknown startup failure')
				break
			lastState[i] = instance.state
		# check boot processes
		for b in range(len(booters)) :
			nodenum = booters[b].nodenum
			while ( booters[b].readConn.poll() ) : 
				msg = booters[b].readConn.recv()
				if (MSG_SUCCESS==msg) : # setup is complete
					statuses[nodenum] = "R" # running
					nRunning = nRunning + 1
				elif (MSG_FAILURE==msg) : # uh oh
					eca.log('startup failure on node %d'%nodenum)
					break
				else : # just a log message
					eca.log(msg)
					last_status_string = "" # interrupt the ... if any

		# and show progress if any
		status_string = ""
		for i in range(len(instances)) :
			status_string = status_string + statuses[i]
		if (status_string != last_status_string) :
			eca.log_no_newline("status, nodes 0-%d: %s"%((len(instances)-1),status_string))
			last_status_string = status_string
		else :
			eca.log_progress() # just put a dot to show we're alive
		if nRunning < len(instances) :
			sleep(wait)
		else :
			break # all ready

	# success?
	for i in range(len(booters)) :
		booters[i].join()
	if ( nRunning != len(instances) ) :
		if ("True"!=eca.getConfig("keepHead","False")) :
			eca.log("error starting cluster, terminating all nodes")
			for j in range(len(instances)) :
				instances[j].terminate()
		else :
			eca.log("error starting cluster, not terminating nodes due to parameter keepHead:True")
		eca.log("exit with error")
		exit(-1)

	eca.log('head node public DNS: %s' % (instances[0].dns_name) )

	for n in range(len(eca.cfgStack)):
		eca.selectConfig(n)
		eca.log('results will be written to S3 file '+eca.getConfig("s3bucketID")+"/"+eca.S3CompatibleString(eca.getConfig("reportFileName")))

	if ("True"!=eca.getConfig("keepHead","False")) :
		eca.info('cluster will self-terminate at conclusion of calculation')
	else :
		eca.log('cluster will not self terminate: keepHead='+eca.getConfig("keepHead","False")+ " and keepClients="+eca.getConfig("keepClients","False"))
	eca.log_no_newline('waiting for results')
	eca.log_progress() # put a dot with no newline
	logfileOld = ""
	results = [""]
	resultsOld = [""]
	for n in range(len(eca.cfgStack)):
		results.extend([""])
		resultsOld.extend([""])
	# TODO: pull down result files as they come available in multi-job runs
	instance = instances[0]
	while instance.state == u'running' :
		sleep( wait )
		instance.update()
	logfile = eca.downloadFromS3IfExists( eca.getConfig( "logFileName" ) )
	if ( logfile != None ) :
		logfileNew = logfile.replace( logfileOld, "" )
		eca.log_no_timestamp( logfileNew )
		logfileOld = logfile
	for n in range(len(eca.cfgStack)):
		eca.selectConfig(n)
		results[n] = eca.downloadFromS3IfExists( eca.getConfig( "reportFileName" ) )
		if ( results[n] != None ) :
			resultsNew = results[n].replace( resultsOld[n], "" )
			eca.log_no_timestamp( resultsNew )
			resultsOld[n] = results[n]
	eca.log( 'head node has shut down.' )   #  Results:' )
	eca.log( "copying head node logs from S3 %s/%s to %s"%(eca.getConfig("s3bucketID"),eca.getConfig( "logFileName" ),localLogFileName))
	eca.downloadFileFromS3(eca.getConfig( "logFileName" ),localLogFileName )
	for n in range(len(eca.cfgStack)):
		eca.selectConfig(n)
		resultsFilename = eca.getConfig( "resultsFilename", "" )
		if (resultsFilename != "") :
			eca.log("copying results from S3 %s/%s to %s"%(eca.getConfig("s3bucketID"),eca.getConfig("reportFileName"),resultsFilename))
			eca.downloadFileFromS3(eca.getConfig("reportFileName"),resultsFilename)

	# also copy over any files indicated as key="resultFile_*"
	# with value="<ec2_filename>;<s3_filename>;<local_filename>"
	for n in range(-1,len(eca.cfgStack)) : # configID -1 is the core config
		for key,val in eca.selectConfig(n).iteritems():
			if (key.startswith("resultFile_")) :
				eca.log("downloading results file "+val.split(";")[2]+"...")
				results = eca.downloadFileFromS3(val.split(";")[1],val.split(";")[2]) # s3name, localname

	eca.log_close()
	# end if main
