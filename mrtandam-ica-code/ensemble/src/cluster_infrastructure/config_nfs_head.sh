#!/bin/bash 

# create and export a shared directory over nfs; the expectation is that
#   this will be run on the head node

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

# establishes NFS communication between this (the head) node and client nodes 

if [ -e /etc/lsb-release ]; then
    # we might be on ubuntu
    . /etc/lsb-release
    # DISTRIB_ID and DISTRIB_RELEASE should now be set
	echo "installing nfs"
	apt-get -y install nfs-kernel-server nfs-common portmap  > /dev/null
	nfs_script="nfs-kernel-server"
elif [ -e /etc/redhat-release ]; then
    grep "CentOS" /etc/redhat-release > /dev/null
    if [ $? -eq 0 ]; then
        DISTRIB_ID="CentOS"
    fi
	echo "portmap: 10.0.0.0/255.0.0.0" >> /etc/hosts.allow
	nfs_script="nfs"
else
    #sanity check
    echo "Distribution not recogized; should be one of"
    echo "Centos or Ubuntu "
fi

if [ ! -e /mnt/nfs-shared ] ; then

	# create the shared shared directory
	mkdir -p /mnt/nfs-shared

	# make it writeable by the clients
	# - not very elegant but it works
	chmod a+rwx /mnt/nfs-shared
	chmod -R a+rw /mnt/nfs-shared
	
	# set up the export info
	echo "/mnt/nfs-shared 10.255.255.255/8(rw,sync)" >> /etc/exports
	exportfs -a
	if [[ $? -ne 0 ]]
	then
	    echo "exportfs failed"
	    exit -1
	else
	    echo "exportfs succeeded"
	fi
	
	echo "restarting nfs server"
	# serve it up
	/etc/init.d/portmap restart > /dev/null
	/etc/init.d/$nfs_script restart > /dev/null
	if [[ $? -ne 0 ]]
	then
	    echo "nfs restart failed"
	    exit -1
	else
	    echo "nfs restart succeeded"
	fi

	echo "nfs server up!"
fi

exit 0
