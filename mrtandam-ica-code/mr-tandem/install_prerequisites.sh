#!/bin/bash
# a script for linux and cygwin to install needed packages for MR-Tandem 
#
# assumes python is installed already

BOTO_VER="2.0rc1"
wget http://pypi.python.org/packages/source/b/boto/boto-$BOTO_VER.tar.gz
tar -xzf boto-$BOTO_VER.tar.gz
cd boto-$BOTO_VER ; ./setup.py install

SJ_VER="2.1.3"
wget http://pypi.python.org/packages/source/s/simplejson/simplejson-$SJ_VER.tar.gz
tar -xzf simplejson-$SJ_VER.tar.gz
cd simplejson-$SJ_VER ; ./setup.py install

# only need hadoop installed for use with non-AWS clusters
# you really, really need to match the hadoop version on your target cluster
#HADOOP_VER="0.20.1+152" # for example, but probably wrong for you
if [ "$HADOOP_VER"!="" ] ; then 
cd /usr/local/ ; wget http://archive.cloudera.com/cdh/testing/hadoop-$HADOOP_VER.tar.gz ; tar -xzf hadoop-$HADOOP_VER.tar.gz
fi