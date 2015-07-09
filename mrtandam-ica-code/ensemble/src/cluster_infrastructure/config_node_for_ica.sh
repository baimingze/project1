#!/bin/bash

#  Copyright (C) 2010, 2011 Insilicos LLC  All Rights Reserved
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


# This script configures a running StarCluster node for use in an ICA cluster.

log() {
    date=`date`
    echo -n "$date -- " 
    echo $1 
}

checkcmd() {
    if [[ $? -eq 0 ]]
    then
	log "ok"
    else
	echo "error"
	exit -1
    fi
}

log "config for ICA"

if [ -e /etc/lsb-release ]; then
    # we might be on ubuntu
    . /etc/lsb-release
    # DISTRIB_ID and DISTRIB_RELEASE should now be set
elif [ -e /etc/redhat-release ]; then
    grep "CentOS" /etc/redhat-release > /dev/null
    if [ $? -eq 0 ]; then
        DISTRIB_ID="CentOS"
    fi
else
    #sanity check
    echo "Centos or Ubuntu only, please"
fi

if [ $DISTRIB_ID = "Ubuntu" ]; then
	log "enabling multiverse in apt-get sources"
	cat /etc/apt/sources.list | grep universe | sed s/universe/multiverse/g > /tmp/aptsources
	cat /tmp/aptsources >> /etc/apt/sources.list
	log "adding CRAN to apt-get sources for R install"
	codename=`lsb_release -a | grep Codename | cut -f 2`
	echo "deb http://cran.fhcrc.org/bin/linux/ubuntu $codename/" >> /etc/apt/sources.list
	checkcmd
	log "updating system"
	apt-get update
	INSTALL_CMD="apt-get"
	FORCE_YES="--force-yes"
	UPDATE_ARG="dist-upgrade"
	CLEAN_ARG="autoremove"
	RUBY_PKGS=libopenssl-ruby
	R_PACKAGE="r-base"
	checkcmd
else
	log "adding CRAN to yum sources for R install"
	INSTALL_CMD="yum"
	FORCE_YES="--skip-broken"
	CLEAN_ARG="clean all"
	LIBUPDATE="ldconfig"
	RUBY_PKGS=openssl-devel
	R_PACKAGE="R"
	rpm -Uvh http://download.fedora.redhat.com/pub/epel/5/i386/epel-release-5-4.noarch.rpm
#	$INSTALL_CMD -y install \
#	gcc \
#	gcc-c++ \
#	curl-devel \
#	perl \
#	libxml2-devel
#	checkcmd
fi

export MPI_ROOT=$(dirname $(dirname `command -v mpirun`))	
if [ $DISTRIB_ID = "Ubuntu" ]; then
	MPI_DEV="libopenmpi-dev"
else
	MPI_DEV="openmpi-devel"
fi
command -v mpicxx &>/dev/null || { $INSTALL_CMD -y install $MPI_DEV ; }


log "installing R"
command -v Rscript &>/dev/null || { $INSTALL_CMD -y $FORCE_YES install $R_PACKAGE ; }
checkcmd

# install Rmpi 0.5-7 on top of openmpi
cd /usr/local/lib/R/site-library
wget http://www.stats.uwo.ca/faculty/yu/Rmpi/download/linux/Rmpi_0.5-8.tar.gz
checkcmd
R CMD INSTALL Rmpi_0.5-8.tar.gz
checkcmd

# From others "This is an evil hack to avoid the tedious adding of
# each compute node host when managing a large cluster...I'm assuming
# these EC2 nodes will only connect to each other and S3, please be
# careful."

cd /etc/ssh
sed -i.bak 's/\#[ \t]*StrictHostKeyChecking[ \t].*/StrictHostKeyChecking no/' ssh_config
checkcmd
cd ~
# ssh config updated
# get boto and simplejson packages using newest Python 2.x you can find
export PYTHON=`which python27`
if [ "" = "$PYTHON" ]; then export PYTHON=`which python26` ; fi
if [ "" = "$PYTHON" ]; then export PYTHON=`which python25` ; fi
if [ "" = "$PYTHON" ]; then export PYTHON=`which python` ; fi
$PYTHON /root/setup.py install
