#
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

####
# IMPORTANT
####

## The hadoop-config.sh tends to get executed by non-Hadoop scripts.
## Those parts expect this script to parse/manipulate $@. In order
## to maintain backward compatibility, this means a surprising
## lack of functions for bits that would be much better off in
## a function.
##
## In other words, yes, there is some bad things happen here and
## unless we break the rest of the ecosystem, we can't change it. :(


# included in all the hadoop scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*
#
# after doing more config, caller should also exec finalize
# function to finish last minute/default configs for
# settings that might be different between daemons & interactive

# you must be this high to ride the ride
if [[ -z "${BASH_VERSINFO}" ]] || [[ "${BASH_VERSINFO}" -lt 3 ]]; then
  echo "Hadoop requires bash v3 or better. Sorry."
  exit 1
fi

# In order to get partially bootstrapped, we need to figure out where
# we are located. Chances are good that our caller has already done
# this work for us, but just in case...

if [[ -z "${HADOOP_LIBEXEC_DIR}" ]]; then
  _hadoop_common_this="${BASH_SOURCE-$0}"
  HADOOP_LIBEXEC_DIR=$(cd -P -- "$(dirname -- "${_hadoop_common_this}")" >/dev/null && pwd -P)
fi

# get our functions defined for usage later
if [[ -f "${HADOOP_LIBEXEC_DIR}/hadoop-functions.sh" ]]; then
  . "${HADOOP_LIBEXEC_DIR}/hadoop-functions.sh"
else
  echo "ERROR: Unable to exec ${HADOOP_LIBEXEC_DIR}/hadoop-functions.sh." 1>&2
  exit 1
fi

# allow overrides of the above and pre-defines of the below
if [[ -f "${HADOOP_LIBEXEC_DIR}/hadoop-layout.sh" ]]; then
  . "${HADOOP_LIBEXEC_DIR}/hadoop-layout.sh"
fi

#
# IMPORTANT! We are not executing user provided code yet!
#

# Let's go!  Base definitions so we can move forward
hadoop_bootstrap_init

# let's find our conf.
#
# first, check and process params passed to us
# we process this in-line so that we can directly modify $@
# if something downstream is processing that directly,
# we need to make sure our params have been ripped out
# note that we do many of them here for various utilities.
# this provides consistency and forces a more consistent
# user experience


# save these off in case our caller needs them
# shellcheck disable=SC2034
HADOOP_USER_PARAMS="$@"

HADOOP_DAEMON_MODE="default"

while [[ -z "${_hadoop_common_done}" ]]; do
  case $1 in
    --buildpaths)
      # shellcheck disable=SC2034
      HADOOP_ENABLE_BUILD_PATHS=true
      shift
    ;;
    --config)
      shift
      confdir=$1
      shift
      if [[ -d "${confdir}" ]]; then
        # shellcheck disable=SC2034
        YARN_CONF_DIR="${confdir}"
        # shellcheck disable=SC2034
        HADOOP_CONF_DIR="${confdir}"
      elif [[ -z "${confdir}" ]]; then
        hadoop_error "ERROR: No parameter provided for --config "
        hadoop_exit_with_usage 1
      else
        hadoop_error "ERROR: Cannot find configuration directory \"${confdir}\""
        hadoop_exit_with_usage 1
      fi
    ;;
    --daemon)
      shift
      HADOOP_DAEMON_MODE=$1
      shift
      if [[ -z "${HADOOP_DAEMON_MODE}" || \
        ! "${HADOOP_DAEMON_MODE}" =~ ^st(art|op|atus)$ ]]; then
        hadoop_error "ERROR: --daemon must be followed by either \"start\", \"stop\", or \"status\"."
        hadoop_exit_with_usage 1
      fi
    ;;
    --help|-help|-h|help|--h|--\?|-\?|\?)
      hadoop_exit_with_usage 0
    ;;
    --hostnames)
      shift
      # shellcheck disable=SC2034
      HADOOP_SLAVE_NAMES="$1"
      shift
    ;;
    --hosts)
      shift
      hadoop_populate_slaves_file "$1"
      shift
    ;;
    *)
      _hadoop_common_done=true
    ;;
  esac
done

hadoop_find_confdir
hadoop_exec_hadoopenv

#
# IMPORTANT! User provided code is now available!
#

# do all the OS-specific startup bits here
# this allows us to get a decent JAVA_HOME,
# call crle for LD_LIBRARY_PATH, etc.
hadoop_os_tricks

hadoop_java_setup

hadoop_basic_init

# inject any sub-project overrides, defaults, etc.
if declare -F hadoop_subproject_init >/dev/null ; then
  hadoop_subproject_init
fi

# get the native libs in there pretty quick
hadoop_add_javalibpath "${HADOOP_PREFIX}/build/native"
hadoop_add_javalibpath "${HADOOP_PREFIX}/${HADOOP_COMMON_LIB_NATIVE_DIR}"

# get the basic java class path for these subprojects
# in as quickly as possible since other stuff
# will definitely depend upon it.
#
# at some point, this will get replaced with something pluggable
# so that these functions can sit in their projects rather than
# common
#
for i in common hdfs yarn mapred
do
  hadoop_add_to_classpath_$i
done

#
# backwards compatibility. new stuff should
# call this when they are ready
#
if [[ -z "${HADOOP_NEW_CONFIG}" ]]; then
  hadoop_finalize
fi
