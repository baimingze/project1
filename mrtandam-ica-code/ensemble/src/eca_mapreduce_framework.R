#!/usr/bin/env Rscript
# TODO copyright and license
# should be called with as 
#   Rscript eca_mapreduce_framework.R <"mapper"|"reducer"|"copier"> <implementation script name> <configFileDotR>
#
# Normally this is done via the eca_launch_mapreduce.py script run on your local machine, but you can call
# this script directly on your local machine when prototyping your hadoop job with commandline and pipes
# (actually eca_launch_mapreduce.py will do that for you if you set the runLocal eca_config parameter to "True")

# minimal config to get us to the point where we can read actual config
eca_config <<- list("verbose"="False","runLocal"="False")
runningAsClient <<- FALSE
runningAsHadoopMapper <<- TRUE  # so logs go to stderr
glob_codeready <<- FALSE

eca_readpipe <- function(fname, nlines=-1) {
   # handle possibly gzipped data
   # we use a pipe here because we may want HDFS access, and R file handling doesn't deal with that
    pipe(eca_construct_pipe_cmd(fname,nlines))
}

eca_heartbeat <- function(textA, textB = "") {
	if ((! glob_codeready ) || eca_verbose()) { 
		cat(paste(date()," ",textA,textB,"\n",sep=""),file=stderr())
	} else if ( ! eca_runLocal() ) { # tag these as hadoop use only
		cat(paste("reporter:status:",textA,textB,sep=""),file=stderr())
	}
}

eca_in_hadoop <- function() {
	nchar(Sys.getenv("job_local_dir"))>0
}

glob_runmode <<- ""
glob_logstr <<- ""

# stuff for HDFS handoff of large objects
n_keyvaluepairs_written <<- 0
glob_keypair_fname <<- ""
glob_keypair_tmpfname <<- ""
glob_keypair_file_conn <<- NULL
glob_keypair_file_conn_pos <<- 0

eca_open_key_value_file <- function() {
	if ( "" == glob_keypair_fname ) { # already open?
		if (eca_in_hadoop()) { # running under Hadoop?
			taskID <- Sys.getenv("mapred_task_id")
			tmpdir <- Sys.getenv("job_local_dir")
			glob_keypair_fname <<- paste(taskID,".",n_keyvaluepairs_written,sep="")
			glob_keypair_tmpfname <<- paste(tmpdir,"/",glob_keypair_fname,sep="")
			hadoopdirname <- Sys.getenv("hadoop_dir")
			if (0==nchar(hadoopdirname)) {
				hadoopdirname <- "/home/hadoop"
			}
			glob_keypair_fname <<- paste("hdfs://",hadoopdirname,"/",glob_keypair_fname,sep="")
		} else { 
			# helpfully cleans up its tempdir on exit, but we need it
			# to persist for next step so use that as name building tool instead
			glob_keypair_tmpfname <<- tempfile()
			tmpdir = dirname(glob_keypair_tmpfname)
			glob_keypair_fname <<- paste(tmpdir,"_",basename(glob_keypair_tmpfname),sep="")
			glob_keypair_tmpfname <<- glob_keypair_fname
			tmpdir = dirname(glob_keypair_tmpfname)
		}
		dir.create(tmpdir, showWarnings = FALSE, recursive = TRUE)
		glob_keypair_file_conn <<- file(glob_keypair_tmpfname,"wb")
		glob_keypair_file_conn_pos <<- 0
	}
}

eca_close_key_value_file <- function() {
	if ("" != glob_keypair_fname) {
		close(glob_keypair_file_conn)
		if (eca_in_hadoop()) { # running under Hadoop?
			# copy local file to HDFS
	eca_log(paste("hadoop dfs -put",glob_keypair_tmpfname,glob_keypair_fname))
			
			p=pipe(paste("hadoop dfs -put",glob_keypair_tmpfname,glob_keypair_fname),"r")
			eca_log(readLines(p))
			close(p)
			unlink(glob_keypair_tmpfname) # kill local file
		}
	}
	glob_keypair_fname <<- ""
	glob_keypair_file_conn_pos <<- 0
	glob_keypair_file_conn <<- NULL
}

eca_framework_write_key_value_pair <- function(id, key, value) {
	if (id != "-1") { # -1 is magic number for final log transmission
		eca_heartbeat("send value from ensemble ",id) # tickle stderr so hadoop knows we're still here
	}
	asciiDump <- rawToChar(serialize(value, NULL, ascii=T))
	dumpLen = nchar(asciiDump)
	totalLen = dumpLen * as.integer(eca_config$ensembleSize) # effect of all ensembles on reducer
	if ((totalLen > 2E6) || (glob_keypair_file_conn_pos > 0)) { # if need to use, or are already using
		# pass big objects through HDFS file system so we don't choke Hadoop sort
		# TODO - get in-memory sort size from hadoop config
		#
		if (glob_keypair_file_conn_pos + dumpLen > 2E9 ) { # pushing R's 2 gig limit
			if (glob_keypair_file_conn_pos > 0) { # we didn't just open that, did we?
				eca_close_key_value_file() # close current and start a new one
			}
		}
		eca_open_key_value_file() # create if not already open
		# pass reducer the name of HDFS file and seek position for deserializing
		cat(key,"\t",id,"@","FILEREF#",glob_keypair_fname,"#",glob_keypair_file_conn_pos,"\n", sep="")
		# write the object to local file (copy to HDFS at the end)
		writeLines(asciiDump,glob_keypair_file_conn)
		glob_keypair_file_conn_pos <<- seek(glob_keypair_file_conn)
	} else { # pass through stdio
		# replace newlines with hashes to avoid confusing hadoop linesorter
		cat(key,"\t",id,"@",gsub("\n","#",asciiDump),"\n", sep = "")
	}
	flush.console() # keep that stdio flowing so hadoop knows we're alive
	n_keyvaluepairs_written <<- (n_keyvaluepairs_written + 1)
}

#
# avoid choking hadoop with large objects by passing references
# to HDFS file containing their serializations
#
keyvalue_filenames <<- c()
keyvalue_files <<- list()
keyvalue_local_filenames <<- list()

eca_get_keyvalue_conn <- function(id_value) {
	fname = strsplit(id_value,"#")[[1]][2]
	localname = ""
	for ( findname in keyvalue_filenames ) {
		if (fname == findname) {
			localname = keyvalue_local_filenames[fname]
			break
		}
	}
	if (localname=="") {
		# first mention of this file
		keyvalue_filenames <<- c(keyvalue_filenames, fname) # note for eventual delete
		if (eca_in_hadoop()) { # need to pull down to local fs?
			localname = tempfile()
			p = pipe(paste("hadoop dfs -get",fname,localname),"rb")
			readLines(p)
			close(p)
		} else {
			localname = fname
		}	
		keyvalue_local_filenames[fname] <<- localname
		keyvalue_files[fname] <<- list(file(localname,"rb"))
	}
	# now seek
	conn = keyvalue_files[fname][[1]]
	seek(conn,as.integer(strsplit(id_value,"#")[[1]][3]))
	conn
}

# remove any tempfiles used in transfer of large key value pairs via filesystem
cleanup_keyvalue_files <- function()
	n = 1
	for ( fname in keyvalue_filenames ) {
		close(keyvalue_files[fname])
		if (eca_in_hadoop()) { # running under Hadoop?
			p=pipe(paste("hadoop dfs -rm",fname),"r")
			readLines(p)
			close(p)
			unlink(keyvalue_local_filenames[fname])
		} else {
			unlink(fname)
		}
		n = n + 1
	}

eca_framework_parse_key_value_pair <- function(line) {
	# split <key>/t<id>@<value>
	key_idvalue <- strsplit(line,"\t")[[1]]
	newKey <<- key_idvalue[1]
	id_value <- strsplit(key_idvalue[2],"@")[[1]]
	newMapperID <<- id_value[1]
	# tickle stderr periodically to let Hadoop know we're not stuck
	if (newMapperID != "-1") {  # -1 is magic number for final log transmission
		eca_heartbeat("receive value from ensemble ",newMapperID) # tickle stderr so hadoop knows we're still here
	}
	flush.console()
	if (length(i <- grep("^FILEREF#",id_value[2]))) { # file reference?
		newValue <<- unserialize(eca_get_keyvalue_conn(id_value[2]))
	} else {
		objStr <- gsub("#","\n",id_value[2]) # convert hashmarks back to newlines
		newValue <<- unserialize(charToRaw(objStr))  # suck it into an R object
	}
	newKey
}

errNotice <- function(ex, name) {
	eca_log("A problem occurred with ",name,": ");
	eca_log(ex)
}

args <- commandArgs(TRUE)
glob_runmode <<- args[1] # "mapper", "reducer", or "copier"
runningAsHadoopMapper <<- ( "mapper" == glob_runmode)

connection <- file("stdin", open = "rb") # read from stdin

if ( "copier" == glob_runmode )	{ # special purpose mapper that copies from S3 to HDFS
	eca_heartbeat("copier") # send a heartbeat to hadoop
	repeat {
		# each line is presumed to be a fully prepared shell command
		line <- readLines(connection, n = 1, warn = FALSE)
		if (length(line) <= 0) {
			break # no more lines
		}
		if (! (is.null(line) || ("NULL"==substr(line,1,4)))) {
			eca_heartbeat(line) # send a heartbeat to hadoop
			system(line)
			eca_heartbeat("ok") # send a heartbeat to hadoop
		}
	}
	eca_heartbeat("done") # send a heartbeat to hadoop
	q()
}


implementation = args[2] # script that contains the implementation, gets source()'d
configFilename = args[3] # name of file with config info written as an R statement
source(configFilename) # populates eca_configs - a list of configs cfg0, cfg1, cfg2 etc
eca_config <<- eca_configs[[args[4]]]

# code common to RMPI and MapReduce frameworks
source(eca_config$frameworkSupportScript)
glob_codeready <<- TRUE

# announce R version in debug mode
eca_debug( version )

# pull in the needed canonical functions provide by the user
eca_heartbeat("source the implementation code") # send a heartbeat to hadoop
source(implementation)  # this is where the heavy lifting is

#
# put preamble in a retry loop - this is typically where HDFS is hot and heavy
#
retries_left = 50
repeat {
	tryCatch( {
		eca_heartbeat("common preamble") # send a heartbeat to hadoop
		common_preamble() # run stuff common to mapper and reducer, if any
		eca_heartbeat("wait for stdin") # send a heartbeat to hadoop
		flush.console()
		# OK, stdin is not at EOF, there is work to do
		eca_heartbeat("stdin open") # send a heartbeat to hadoop

		# running as mapper, or reducer?
		if ( "mapper" == glob_runmode )	{
			# running as mapper
			eca_heartbeat("client preamble") # send a heartbeat to hadoop
			client_preamble() # run mapper setup, if any
		} else {
			# running as reducer
			eca_heartbeat("head preamble") # send a heartbeat to hadoop
			head_preamble() # run reducer setup, if any
		}
	}
	, interrupt = function(ex) { errNotice(ex, "common_preamble") }
	, error = function(ex) { errNotice(ex, "common_preamble") }
	, finally = {
		break
	} ) # tryCatch()
	retries_left = retries_left-1
	if (retries_left <= 0) {
		break
	}
	gc() # tidy up any memory used in failed attempt
	Sys.sleep(3) # assume HDFS is still catching up - wait patiently for writer
} # repeat
	
# running as mapper, or reducer?
eca_heartbeat(glob_runmode,"main loop") # send a heartbeat to hadoop
repeat {
	line <- readLines(connection, n = 1, warn = FALSE)
	if (length(line) <= 0) {
		break # no more lines
	}
	if (! (is.null(line) || ("NULL"==substr(line,1,4)))) {
		if ( "mapper" == glob_runmode )	{
			randSeedVal= as.numeric(line) + 1
			result = client_ensemblecalc(randSeedVal) # mapper's main action
			# serialize to stdout, with just one key so single head gets everything
			eca_framework_write_key_value_pair(randSeedVal,"1",result)
		} else {
			eca_framework_parse_key_value_pair(line) # sets newKey, newMapperID, newValue
			if ("-1"==newMapperID) {
				# client is shutting down, transferring its log
				eca_heartbeat("begin log sent from mapper on shutdown:")
				cat(newValue)
				eca_heartbeat("end log sent from mapper on shutdown")
			} else {
				head_resulthandler(newValue, newMapperID) # reducer's main action
			}
		}
	}
}

if ( "mapper" == glob_runmode )	{
	eca_heartbeat("client postscript") # send a heartbeat to hadoop
	client_postscript() # mapper cleanup, if any
} else {
	eca_heartbeat("head postscript") # send a heartbeat to hadoop
	head_postscript() # reducer cleanup, if any
}

# run stuff common to mapper and reducer, if any
eca_heartbeat("common_postscript")
common_postscript() 

eca_heartbeat("close connection") # send a heartbeat to hadoop

if ( "mapper" == glob_runmode )	{
	if (eca_verbose() && !eca_runLocal()) {
		# send accumulated log to reducer
		eca_framework_write_key_value_pair(-1,"1",glob_logstr)
	}
	eca_close_key_value_file() # close out HDFS transfer of large objects, if any
}
close(connection)

if ( "reducer" == glob_runmode )	{
	cleanup_keyvalue_files()
}
q()