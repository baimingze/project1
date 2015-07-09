#!/bin/bash

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

# configure a member of the cluster to see the shared directory on a main node;
# assumes that main node's IP address is passed as the first parameter to this script

logFileName=/var/log/eca-nfs-client.log

log() {
    date=`date`
    echo -n "$date -- " >> $logFileName
    echo $1 >> $logFileName
}

control_node_ip=$1
shared_dir=/mnt/nfs-shared

log "beginning nfs client configuration"
log "nfs server ip address is $control_node_ip"
log "nfs shared directory will be $shared_dir"

# unmount just in case this isn't first call
umount $shared_dir > /dev/null 
# first time here?
log "checking for $shared_dir in fstab"
grep "$shared_dir" /etc/fstab > /dev/null
if [ $? -ne 0 ]
then
    # add
    log ".. not found in fstab, adding"
    echo "$control_node_ip:$shared_dir $shared_dir nfs rw,rsize=8192,wsize=8192,timeo=14,intr 0 0" >> /etc/fstab
    log "..added fstab"
else
   # update
    log ".. found in fstab, editing"
    sed -i "s#.*\:/.*/nfs-shared#$control_node_ip\:$shared_dir $shared_dir#g" /etc/fstab
    log "..edited fstab"
fi

mkdir -p $shared_dir
log "creating $shared_dir locally"

success=-1
while [[ $success -ne 0 ]]
do
    log "restarting portmap"
    /etc/init.d/portmap restart > /dev/null
    if [[ $? -ne 0 ]]
    then
	log "portmap restart failed, waiting and trying again"
	sleep 10
    else
	log "portmap restart succeeded"
	success=0
    fi
done

# some nfs installs (Ubuntu 10) don't have nfs-common
if [ -e /etc/init.d/nfs-common ] ; then
    log "waiting for nfs-common"
	success=-1
	while [[ $success -ne 0 ]]
	do
		log "restarting nfs-common"
		/etc/init.d/nfs-common restart > /dev/null
		if [[ $? -ne 0 ]]
		then
		log "nfs restart failed, waiting and trying again"
		sleep 10
		else
		log "nfs restart succeeded"
		success=0
		fi
	done
fi

success=-1
while [[ $success -ne 0 ]]
do
    log "mounting nfs directory"
    mount $shared_dir  > /dev/null
    if [[ $? -ne 0 ]]
    then
	log "mount restart failed, waiting and trying again"
	sleep 10
    else
	log "mount succeeded"
	success=0
    fi
done


log "testing shared directory $shared_dir"

testfile=`mktemp -q $shared_dir/deleteme.XXXXXX`
if [[ $? -ne 0 ]]
then
    echo "Can't create temp file, exiting"
    exit -1
fi

echo "this is a test" > $testfile
if [[ $? -ne 0 ]]
then
    log "nfs test failed!"
    exit -1
fi

log "nfs test successed!"
rm $testfile

log "nfs client successfully activated"

log "exiting with success"
exit 0
