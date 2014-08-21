#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set Hadoop-specific environment variables here.

##
## THIS FILE ACTS AS THE MASTER FILE FOR ALL HADOOP PROJECTS.
## SETTINGS HERE WILL BE READ BY ALL HADOOP COMMANDS.  THEREFORE,
## ONE CAN USE THIS FILE TO SET YARN, HDFS, AND MAPREDUCE
## CONFIGURATION OPTIONS INSTEAD OF xxx-env.sh.
##
## Precedence rules:
##
## {yarn-env.sh|hdfs-env.sh} > hadoop-env.sh > hard-coded defaults
##
## {YARN_xyz|HDFS_xyz} > HADOOP_xyz > hard-coded defaults
##

# Many of the options here are built from the perspective that users
# may want to provide OVERWRITING values on the command line.
# For example:
#
#  JAVA_HOME=/usr/java/testing hdfs dfs -ls
#
# Therefore, the vast majority (BUT NOT ALL!) of these defaults
# are configured for substitution and not append.  If you would
# like append, you'll # need to modify this file accordingly.

###
# Generic settings for HADOOP
###

# Technically, the only required environment variable is JAVA_HOME.
# All others are optional.  However, our defaults are probably not
# your defaults.  Many sites configure these options outside of Hadoop,
# such as in /etc/profile.d

# The java implementation to use.
export JAVA_HOME=${JAVA_HOME:-"hadoop-env.sh is not configured"}

# Location of Hadoop's configuration information.  i.e., where this
# file is probably living.  You will almost certainly want to set
# this in /etc/profile.d or equivalent.
# export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop

# The maximum amount of heap to use, in MB. Default is 1024.
# export HADOOP_HEAPSIZE=1024

# Extra Java runtime options for all Hadoop commands. We don't support
# IPv6 yet/still, so by default we set preference to IPv4.
# export HADOOP_OPTS="-Djava.net.preferIPv4Stack=true"

# Some parts of the shell code may do special things dependent upon
# the operating system.  We have to set this here. See the next
# section as to why....
export HADOOP_OS_TYPE=${HADOOP_OS_TYPE:-$(uname -s)}


# Under certain conditions, Java on OS X will throw SCDynamicStore errors
# in the system logs.
# See HADOOP-8719 for more information.  If you need Kerberos
# support on OS X, you'll want to change/remove this extra bit.
case ${HADOOP_OS_TYPE} in
  Darwin*)
    export HADOOP_OPTS="${HADOOP_OPTS} -Djava.security.krb5.realm= "
    export HADOOP_OPTS="${HADOOP_OPTS} -Djava.security.krb5.kdc= "
    export HADOOP_OPTS="${HADOOP_OPTS} -Djava.security.krb5.conf= "
  ;;
esac

# Extra Java runtime options for Hadoop clients (i.e., hdfs dfs -blah)
# These get added to HADOOP_OPTS for such commands.  In most cases,
# this should be left empty and let users supply it on the
# command line.
# extra HADOOP_CLIENT_OPTS=""

#
# A note about classpaths.
#
# The classpath is configured such that entries are stripped prior
# to handing to Java based either upon duplication or non-existence.
# Wildcards and/or directories are *NOT* expanded as the
# de-duplication is fairly simple.  So if two directories are in
# the classpath that both contain awesome-methods-1.0.jar,
# awesome-methods-1.0.jar will still be seen by java.  But if
# the classpath specifically has awesome-methods-1.0.jar from the
# same directory listed twice, the last one will be removed.
#

# An additional, custom CLASSPATH.  This is really meant for
# end users, but as an administrator, one might want to push
# something extra in here too, such as the jar to the topology
# method.  Just be sure to append to the existing HADOOP_USER_CLASSPATH
# so end users have a way to add stuff.
# export HADOOP_USER_CLASSPATH="/some/cool/path/on/your/machine"

# Should HADOOP_USER_CLASSPATH be first in the official CLASSPATH?
# export HADOOP_USER_CLASSPATH_FIRST="yes"

# If HADOOP_USE_CLIENT_CLASSLOADER is set, HADOOP_CLASSPATH along with the main
# jar are handled by a separate isolated client classloader. If it is set,
# HADOOP_USER_CLASSPATH_FIRST is ignored. Can be defined by doing
# export HADOOP_USE_CLIENT_CLASSLOADER=true

# HADOOP_CLIENT_CLASSLOADER_SYSTEM_CLASSES overrides the default definition of
# system classes for the client classloader when HADOOP_USE_CLIENT_CLASSLOADER
# is enabled. Names ending in '.' (period) are treated as package names, and
# names starting with a '-' are treated as negative matches. For example,
# export HADOOP_CLIENT_CLASSLOADER_SYSTEM_CLASSES="-org.apache.hadoop.UserClass,java.,javax.,org.apache.hadoop."

###
# Options for remote shell connectivity
###

# There are some optional components of hadoop that allow for
# command and control of remote hosts.  For example,
# start-dfs.sh will attempt to bring up all NNs, DNS, etc.

# Options to pass to SSH when one of the "log into a host and
# start/stop daemons" scripts is executed
# export HADOOP_SSH_OPTS="-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10s"

# The built-in ssh handler will limit itself to 10 simultaneous connections.
# For pdsh users, this sets the fanout size ( -f )
# Change this to increase/decrease as necessary.
# export HADOOP_SSH_PARALLEL=10

# Filename which contains all of the hosts for any remote execution
# helper scripts # such as slaves.sh, start-dfs.sh, etc.
# export HADOOP_SLAVES="${HADOOP_CONF_DIR}/slaves"

###
# Options for all daemons
###
#

#
# You can define variables right here and then re-use them later on.
# For example, it is common to use the same garbage collection settings
# for all the daemons.  So we could define:
#
# export HADOOP_GC_SETTINGS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps"
#
# .. and then use it as per the b option under the namenode.

# Where (primarily) daemon log files are stored.
# $HADOOP_PREFIX/logs by default.
# export HADOOP_LOG_DIR=${HADOOP_PREFIX}/logs

# A string representing this instance of hadoop. $USER by default.
# This is used in writing log and pid files, so keep that in mind!
# export HADOOP_IDENT_STRING=$USER

# How many seconds to pause after stopping a daemon
# export HADOOP_STOP_TIMEOUT=5

# Where pid files are stored.  /tmp by default.
# export HADOOP_PID_DIR=/tmp

# Default log level and output location
# This sets the hadoop.root.logger property
# export HADOOP_ROOT_LOGGER=INFO,console

# Default log level for daemons spawned explicitly by hadoop-daemon.sh
# This sets the hadoop.root.logger property
# export HADOOP_DAEMON_ROOT_LOGGER=INFO,RFA

# Default log level and output location for security-related messages.
# It sets -Dhadoop.security.logger on the command line.
# You will almost certainly want to change this on a per-daemon basis!
# export HADOOP_SECURITY_LOGGER=INFO,NullAppender

# Default log level for file system audit messages.
# It sets -Dhdfs.audit.logger on the command line.
# You will almost certainly want to change this on a per-daemon basis!
# export HADOOP_AUDIT_LOGGER=INFO,NullAppender

# Default process priority level
# Note that sub-processes will also run at this level!
# export HADOOP_NICENESS=0

# Default name for the service level authorization file
# export HADOOP_POLICYFILE="hadoop-policy.xml"

###
# Secure/privileged execution
###

#
# Out of the box, Hadoop uses jsvc from Apache Commons to launch daemons
# on privileged ports.  This functionality can be replaced by providing
# custom functions.  See hadoop-functions.sh for more information.
#

# The jsvc implementation to use. Jsvc is required to run secure datanodes.
# export JSVC_HOME=/usr/bin

#
# This directory contains pids for secure and privileged processes.
#export HADOOP_SECURE_PID_DIR=${HADOOP_PID_DIR}

#
# This directory contains the logs for secure and privileged processes.
# export HADOOP_SECURE_LOG=${HADOOP_LOG_DIR}

#
# When running a secure daemon, the default value of HADOOP_IDENT_STRING
# ends up being a bit bogus.  Therefore, by default, the code will
# replace HADOOP_IDENT_STRING with HADOOP_SECURE_xx_USER.  If you want
# to keep HADOOP_IDENT_STRING untouched, then uncomment this line.
# export HADOOP_SECURE_IDENT_PRESERVE="true"

###
# NameNode specific parameters
###
# Specify the JVM options to be used when starting the NameNode.
# These options will be appended to the options specified as HADOOP_OPTS
# and therefore may override any similar flags set in HADOOP_OPTS
#
# a) Set JMX options
# export HADOOP_NAMENODE_OPTS="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=1026"
#
# b) Set garbage collection logs
# export HADOOP_NAMENODE_OPTS="${HADOOP_GC_SETTINGS} -Xloggc:${HADOOP_LOG_DIR}/gc-rm.log-$(date +'%Y%m%d%H%M')"
#
# c) ... or set them directly
# export HADOOP_NAMENODE_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -Xloggc:${HADOOP_LOG_DIR}/gc-rm.log-$(date +'%Y%m%d%H%M')"

# this is the default:
# export HADOOP_NAMENODE_OPTS="-Dhadoop.security.logger=INFO,RFAS -Dhdfs.audit.logger=INFO,NullAppender"

###
# SecondaryNameNode specific parameters
###
# Specify the JVM options to be used when starting the SecondaryNameNode.
# These options will be appended to the options specified as HADOOP_OPTS
# and therefore may override any similar flags set in HADOOP_OPTS
#
# This is the default:
# export HADOOP_SECONDARYNAMENODE_OPTS="-Dhadoop.security.logger=INFO,RFAS -Dhdfs.audit.logger=INFO,NullAppender"

###
# DataNode specific parameters
###
# Specify the JVM options to be used when starting the DataNode.
# These options will be appended to the options specified as HADOOP_OPTS
# and therefore may override any similar flags set in HADOOP_OPTS
#
# This is the default:
# export HADOOP_DATANODE_OPTS="-Dhadoop.security.logger=ERROR,RFAS"

# On secure datanodes, user to run the datanode as after dropping privileges
# This **MUST** be uncommented to enable secure HDFS!
# export HADOOP_SECURE_DN_USER=hdfs

# Supplemental options for secure datanodes
# By default, we use jsvc which needs to know to launch a
# server jvm.
# export HADOOP_DN_SECURE_EXTRA_OPTS="-jvm server"

# Where datanode log files are stored in the secure data environment.
# export HADOOP_SECURE_DN_LOG_DIR=${HADOOP_SECURE_LOG_DIR}

# Where datanode pid files are stored in the secure data environment.
# export HADOOP_SECURE_DN_PID_DIR=${HADOOP_SECURE_PID_DIR}

###
# NFS3 Gateway specific parameters
###
# Specify the JVM options to be used when starting the NFS3 Gateway.
# These options will be appended to the options specified as HADOOP_OPTS
# and therefore may override any similar flags set in HADOOP_OPTS
#
# export HADOOP_NFS3_OPTS=""

# Specify the JVM options to be used when starting the Hadoop portmapper.
# These options will be appended to the options specified as HADOOP_OPTS
# and therefore may override any similar flags set in HADOOP_OPTS
#
# export HADOOP_PORTMAP_OPTS="-Xmx512m"

# Supplemental options for priviliged gateways
# By default, we use jsvc which needs to know to launch a
# server jvm.
# export HADOOP_NFS3_SECURE_EXTRA_OPTS="-jvm server"

# On privileged gateways, user to run the gateway as after dropping privileges
# export HADOOP_PRIVILEGED_NFS_USER=nfsserver

###
# ZKFailoverController specific parameters
###
# Specify the JVM options to be used when starting the ZKFailoverController.
# These options will be appended to the options specified as HADOOP_OPTS
# and therefore may override any similar flags set in HADOOP_OPTS
#
# export HADOOP_ZKFC_OPTS=""

###
# QuorumJournalNode specific parameters
###
# Specify the JVM options to be used when starting the QuorumJournalNode.
# These options will be appended to the options specified as HADOOP_OPTS
# and therefore may override any similar flags set in HADOOP_OPTS
#
# export HADOOP_JOURNALNODE_OPTS=""

###
# HDFS Balancer specific parameters
###
# Specify the JVM options to be used when starting the HDFS Balancer.
# These options will be appended to the options specified as HADOOP_OPTS
# and therefore may override any similar flags set in HADOOP_OPTS
#
# export HADOOP_BALANCER_OPTS=""

###
# Advanced Users Only!
###

#
# When building Hadoop, you can add the class paths to your commands
# via this special env var:
# HADOOP_ENABLE_BUILD_PATHS="true"

# You can do things like replace parts of the shell underbelly.
# Most of this code is in hadoop-functions.sh.
#
#
# For example, if you want to add compression to the rotation
# menthod for the .out files that daemons generate, you can do
# that by redefining the hadoop_rotate_log function by
# uncommenting this code block:

#function hadoop_rotate_log
#{
#  #
#  # log rotation (mainly used for .out files)
#  # Users are likely to replace this one for something
#  # that gzips or uses dates or who knows what.
#  #
#  # be aware that &1 and &2 might go through here
#  # so don't do anything too crazy...
#  #
#  local log=$1;
#  local num=${2:-5};
#
#  if [[ -f "${log}" ]]; then # rotate logs
#    while [[ ${num} -gt 1 ]]; do
#      #shellcheck disable=SC2086
#      let prev=${num}-1
#      if [[ -f "${log}.${prev}" ]]; then
#        mv "${log}.${prev}" "${log}.${num}"
#      fi
#      num=${prev}
#    done
#    mv "${log}" "${log}.${num}"
#    gzip -9 "${log}.${num}"
#  fi
#}
#
#
# Another example:  finding java
#
# By default, Hadoop assumes that $JAVA_HOME is always defined
# outside of its configuration. Eons ago, Apple standardized
# on a helper program called java_home to find it for you.
#
#function hadoop_java_setup
#{
#
#  if [[ -z "${JAVA_HOME}" ]]; then
#     case $HADOOP_OS_TYPE in
#       Darwin*)
#          JAVA_HOME=$(/usr/libexec/java_home)
#          ;;
#     esac
#  fi
#
#  # Bail if we did not detect it
#  if [[ -z "${JAVA_HOME}" ]]; then
#    echo "ERROR: JAVA_HOME is not set and could not be found." 1>&2
#    exit 1
#  fi
#
#  if [[ ! -d "${JAVA_HOME}" ]]; then
#     echo "ERROR: JAVA_HOME (${JAVA_HOME}) does not exist." 1>&2
#     exit 1
#  fi
#
#  JAVA="${JAVA_HOME}/bin/java"
#
#  if [[ ! -x ${JAVA} ]]; then
#    echo "ERROR: ${JAVA} is not executable." 1>&2
#    exit 1
#  fi
#  JAVA_HEAP_MAX=-Xmx1g
#  HADOOP_HEAPSIZE=${HADOOP_HEAPSIZE:-128}
#
#  # check envvars which might override default args
#  if [[ -n "$HADOOP_HEAPSIZE" ]]; then
#    JAVA_HEAP_MAX="-Xmx${HADOOP_HEAPSIZE}m"
#  fi
#}


