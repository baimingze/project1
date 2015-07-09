#!/opt/local/bin/python

# simple script to demonstrate use of ECA
import os
import subprocess
helpdocname = os.path.abspath("../doc/ECA_QuickStartGuide.pdf")

if not os.path.isfile("general_config.json") :
	print "Not quite ready yet - you first need to copy general_config_template.json to general_config.json, and add your AWS credentials there."
	if ("y"==raw_input("Would you like to see the quick start guide for more information? (y or n) ")) :
		print "opening "+helpdocname
		subprocess.Popen(helpdocname,shell=True)
	exit(1)

while True :
	mode = raw_input("Run locally(1), on EC2(2), or EMR(3)?  (Enter 1, 2, 3, or q to quit):")
	commandline = ""
	if ("1"==mode) :
		print "using local R installation"
		commandline = 'eca_launch_rmpi.py general_config.json decision_tree_satimage_job_config.json --local'
	elif ("2"==mode) :
		print "using RMPI on Amazon EC2"
		commandline = 'eca_launch_rmpi.py general_config.json decision_tree_satimage_job_config.json'
	elif ("3"==mode) :
		print "using MapReduce on Amazon EMR"
		commandline = 'eca_launch_mapreduce.py general_config.json decision_tree_satimage_job_config.json'
	elif ("q"==mode) :
		exit(0)
	if ("" != commandline) :
		import platform
		if ( not platform.system().startswith('Windows') ) :
			commandline = "./"+commandline
		print "command line is: "+commandline
		if ( 0 != subprocess.call(commandline,shell=True)) :
			print 'command failed'
			if ("1"==mode) : # local run
				print "is R installed on this computer?"
			print 'see '+helpdocname+' for help'
			exit(1)
		print 'command "'+commandline+'" completed!'
		exit(0)
	else :
		print("sorry, didn't understand that - please enter 1, 2, 3, or q")

