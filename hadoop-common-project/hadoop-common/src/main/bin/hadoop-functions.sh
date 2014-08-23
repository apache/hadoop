#!/bin/bash
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

function hadoop_error
{
  # NOTE: This function is not user replaceable.

  echo "$*" 1>&2
}

function hadoop_bootstrap_init
{
  # NOTE: This function is not user replaceable.

  # the root of the Hadoop installation
  # See HADOOP-6255 for the expected directory structure layout
  
  # By now, HADOOP_LIBEXEC_DIR should have been defined upstream
  # We can piggyback off of that to figure out where the default
  # HADOOP_FREFIX should be.  This allows us to run without
  # HADOOP_PREFIX ever being defined by a human! As a consequence
  # HADOOP_LIBEXEC_DIR now becomes perhaps the single most powerful
  # env var within Hadoop.
  if [[ -z "${HADOOP_LIBEXEC_DIR}" ]]; then
    hadoop_error "HADOOP_LIBEXEC_DIR is not defined.  Exiting."
    exit 1
  fi
  HADOOP_DEFAULT_PREFIX=$(cd -P -- "${HADOOP_LIBEXEC_DIR}/.." >/dev/null && pwd -P)
  HADOOP_PREFIX=${HADOOP_PREFIX:-$HADOOP_DEFAULT_PREFIX}
  export HADOOP_PREFIX
  
  #
  # short-cuts. vendors may redefine these as well, preferably
  # in hadoop-layouts.sh
  #
  HADOOP_COMMON_DIR=${HADOOP_COMMON_DIR:-"share/hadoop/common"}
  HADOOP_COMMON_LIB_JARS_DIR=${HADOOP_COMMON_LIB_JARS_DIR:-"share/hadoop/common/lib"}
  HADOOP_COMMON_LIB_NATIVE_DIR=${HADOOP_COMMON_LIB_NATIVE_DIR:-"lib/native"}
  HDFS_DIR=${HDFS_DIR:-"share/hadoop/hdfs"}
  HDFS_LIB_JARS_DIR=${HDFS_LIB_JARS_DIR:-"share/hadoop/hdfs/lib"}
  YARN_DIR=${YARN_DIR:-"share/hadoop/yarn"}
  YARN_LIB_JARS_DIR=${YARN_LIB_JARS_DIR:-"share/hadoop/yarn/lib"}
  MAPRED_DIR=${MAPRED_DIR:-"share/hadoop/mapreduce"}
  MAPRED_LIB_JARS_DIR=${MAPRED_LIB_JARS_DIR:-"share/hadoop/mapreduce/lib"}
  # setup a default TOOL_PATH
  TOOL_PATH=${TOOL_PATH:-${HADOOP_PREFIX}/share/hadoop/tools/lib/*}

  export HADOOP_OS_TYPE=${HADOOP_OS_TYPE:-$(uname -s)}

  
  # defaults
  export HADOOP_OPTS=${HADOOP_OPTS:-"-Djava.net.preferIPv4Stack=true"}
}

function hadoop_find_confdir
{
  # NOTE: This function is not user replaceable.

  # Look for the basic hadoop configuration area.
  #
  #
  # An attempt at compatibility with some Hadoop 1.x
  # installs.
  if [[ -e "${HADOOP_PREFIX}/conf/hadoop-env.sh" ]]; then
    DEFAULT_CONF_DIR="conf"
  else
    DEFAULT_CONF_DIR="etc/hadoop"
  fi
  export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-${HADOOP_PREFIX}/${DEFAULT_CONF_DIR}}"
}

function hadoop_exec_hadoopenv
{
  # NOTE: This function is not user replaceable.

  if [[ -z "${HADOOP_ENV_PROCESSED}" ]]; then
    if [[ -f "${HADOOP_CONF_DIR}/hadoop-env.sh" ]]; then
      export HADOOP_ENV_PROCESSED=true
      . "${HADOOP_CONF_DIR}/hadoop-env.sh"
    fi
  fi
}


function hadoop_basic_init
{
  # Some of these are also set in hadoop-env.sh.
  # we still set them here just in case hadoop-env.sh is
  # broken in some way, set up defaults, etc.
  #
  # but it is important to note that if you update these
  # you also need to update hadoop-env.sh as well!!!
  
  # CLASSPATH initially contains $HADOOP_CONF_DIR
  CLASSPATH="${HADOOP_CONF_DIR}"
  
  if [[ -z "${HADOOP_COMMON_HOME}" ]] &&
  [[ -d "${HADOOP_PREFIX}/${HADOOP_COMMON_DIR}" ]]; then
    export HADOOP_COMMON_HOME="${HADOOP_PREFIX}"
  fi
  
  # default policy file for service-level authorization
  HADOOP_POLICYFILE=${HADOOP_POLICYFILE:-"hadoop-policy.xml"}
  
  # define HADOOP_HDFS_HOME
  if [[ -z "${HADOOP_HDFS_HOME}" ]] &&
  [[ -d "${HADOOP_PREFIX}/${HDFS_DIR}" ]]; then
    export HADOOP_HDFS_HOME="${HADOOP_PREFIX}"
  fi
  
  # define HADOOP_YARN_HOME
  if [[ -z "${HADOOP_YARN_HOME}" ]] &&
  [[ -d "${HADOOP_PREFIX}/${YARN_DIR}" ]]; then
    export HADOOP_YARN_HOME="${HADOOP_PREFIX}"
  fi
  
  # define HADOOP_MAPRED_HOME
  if [[ -z "${HADOOP_MAPRED_HOME}" ]] &&
  [[ -d "${HADOOP_PREFIX}/${MAPRED_DIR}" ]]; then
    export HADOOP_MAPRED_HOME="${HADOOP_PREFIX}"
  fi
  
  HADOOP_IDENT_STRING=${HADOP_IDENT_STRING:-$USER}
  HADOOP_LOG_DIR=${HADOOP_LOG_DIR:-"${HADOOP_PREFIX}/logs"}
  HADOOP_LOGFILE=${HADOOP_LOGFILE:-hadoop.log}
  HADOOP_NICENESS=${HADOOP_NICENESS:-0}
  HADOOP_STOP_TIMEOUT=${HADOOP_STOP_TIMEOUT:-5}
  HADOOP_PID_DIR=${HADOOP_PID_DIR:-/tmp}
  HADOOP_ROOT_LOGGER=${HADOOP_ROOT_LOGGER:-INFO,console}
  HADOOP_DAEMON_ROOT_LOGGER=${HADOOP_DAEMON_ROOT_LOGGER:-INFO,RFA}
  HADOOP_SECURITY_LOGGER=${HADOOP_SECURITY_LOGGER:-INFO,NullAppender}
  HADOOP_HEAPSIZE=${HADOOP_HEAPSIZE:-1024}
  HADOOP_SSH_OPTS=${HADOOP_SSH_OPTS:-"-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10s"}
  HADOOP_SECURE_LOG_DIR=${HADOOP_SECURE_LOG_DIR:-${HADOOP_LOG_DIR}}
  HADOOP_SECURE_PID_DIR=${HADOOP_SECURE_PID_DIR:-${HADOOP_PID_DIR}}
  HADOOP_SSH_PARALLEL=${HADOOP_SSH_PARALLEL:-10}
}

function hadoop_populate_slaves_file()
{
  # NOTE: This function is not user replaceable.

  local slavesfile=$1
  shift
  if [[ -f "${slavesfile}" ]]; then
    # shellcheck disable=2034
    HADOOP_SLAVES="${slavesfile}"
  elif [[ -f "${HADOOP_CONF_DIR}/${slavesfile}" ]]; then
    # shellcheck disable=2034
    HADOOP_SLAVES="${HADOOP_CONF_DIR}/${slavesfile}"
    # shellcheck disable=2034
    YARN_SLAVES="${HADOOP_CONF_DIR}/${slavesfile}"
  else
    hadoop_error "ERROR: Cannot find hosts file \"${slavesfile}\""
    hadoop_exit_with_usage 1
  fi
}

function hadoop_rotate_log
{
  #
  # log rotation (mainly used for .out files)
  # Users are likely to replace this one for something
  # that gzips or uses dates or who knows what.
  #
  # be aware that &1 and &2 might go through here
  # so don't do anything too crazy...
  #
  local log=$1;
  local num=${2:-5};
  
  if [[ -f "${log}" ]]; then # rotate logs
    while [[ ${num} -gt 1 ]]; do
      #shellcheck disable=SC2086
      let prev=${num}-1
      if [[ -f "${log}.${prev}" ]]; then
        mv "${log}.${prev}" "${log}.${num}"
      fi
      num=${prev}
    done
    mv "${log}" "${log}.${num}"
  fi
}

function hadoop_actual_ssh
{
  # we are passing this function to xargs
  # should get hostname followed by rest of command line
  local slave=$1
  shift
  
  # shellcheck disable=SC2086
  ssh ${HADOOP_SSH_OPTS} ${slave} $"${@// /\\ }" 2>&1 | sed "s/^/$slave: /"
}

function hadoop_connect_to_hosts
{
  # shellcheck disable=SC2124
  local params="$@"
  
  #
  # ssh (or whatever) to a host
  #
  # User can specify hostnames or a file where the hostnames are (not both)
  if [[ -n "${HADOOP_SLAVES}" && -n "${HADOOP_SLAVE_NAMES}" ]] ; then
    hadoop_error "ERROR: Both HADOOP_SLAVES and HADOOP_SLAVE_NAME were defined. Aborting."
    exit 1
  fi
  
  if [[ -n "${HADOOP_SLAVE_NAMES}" ]] ; then
    SLAVE_NAMES=${HADOOP_SLAVE_NAMES}
  else
    SLAVE_FILE=${HADOOP_SLAVES:-${HADOOP_CONF_DIR}/slaves}
  fi
  
  # if pdsh is available, let's use it.  otherwise default
  # to a loop around ssh.  (ugh)
  if [[ -e '/usr/bin/pdsh' ]]; then
    if [[ -z "${HADOOP_SLAVE_NAMES}" ]] ; then
      # if we were given a file, just let pdsh deal with it.
      # shellcheck disable=SC2086
      PDSH_SSH_ARGS_APPEND="${HADOOP_SSH_OPTS}" pdsh \
      -f "${HADOOP_SSH_PARALLEL}" -w ^"${SLAVE_FILE}" $"${@// /\\ }" 2>&1
    else
      # no spaces allowed in the pdsh arg host list
      # shellcheck disable=SC2086
      SLAVE_NAMES=$(echo ${SLAVE_NAMES} | tr -s ' ' ,)
      PDSH_SSH_ARGS_APPEND="${HADOOP_SSH_OPTS}" pdsh \
      -f "${HADOOP_SSH_PARALLEL}" -w "${SLAVE_NAMES}" $"${@// /\\ }" 2>&1
    fi
  else
    if [[ -z "${SLAVE_NAMES}" ]]; then
      SLAVE_NAMES=$(sed 's/#.*$//;/^$/d' "${SLAVE_FILE}")
    fi
    
    # quoting here gets tricky. it's easier to push it into a function
    # so that we don't have to deal with it. However...
    # xargs can't use a function so instead we'll export it out
    # and force it into a subshell
    # moral of the story: just use pdsh.
    export -f hadoop_actual_ssh
    export HADOOP_SSH_OPTS
    echo "${SLAVE_NAMES}" | \
    xargs -n 1 -P"${HADOOP_SSH_PARALLEL}" \
    -I {} bash -c --  "hadoop_actual_ssh {} ${params}"
    wait
  fi
}

function hadoop_add_param
{
  #
  # general param dedupe..
  # $1 is what we are adding to
  # $2 is the name of what we want to add (key)
  # $3 is the key+value of what we're adding
  #
  # doing it this way allows us to support all sorts of
  # different syntaxes, just so long as they are space
  # delimited
  #
  if [[ ! ${!1} =~ $2 ]] ; then
    # shellcheck disable=SC2086
    eval $1="'${!1} $3'"
  fi
}

function hadoop_add_classpath
{
  # two params:
  # $1 = directory, file, wildcard, whatever to add
  # $2 = before or after, which determines where in the
  #      classpath this object should go. default is after
  # return 0 = success
  # return 1 = failure (duplicate, doesn't exist, whatever)
  
  # However, with classpath (& JLP), we can do dedupe
  # along with some sanity checking (e.g., missing directories)
  # since we have a better idea of what is legal
  #
  # for wildcard at end, we can
  # at least check the dir exists
  if [[ $1 =~ ^.*\*$ ]]; then
    local mp=$(dirname "$1")
    if [[ ! -d "${mp}" ]]; then
      return 1
    fi
    
    # no wildcard in the middle, so check existence
    # (doesn't matter *what* it is)
  elif [[ ! $1 =~ ^.*\*.*$ ]] && [[ ! -e "$1" ]]; then
    return 1
  fi
  
  if [[ -z "${CLASSPATH}" ]]; then
    CLASSPATH=$1
  elif [[ ":${CLASSPATH}:" != *":$1:"* ]]; then
    if [[ "$2" = "before" ]]; then
      CLASSPATH="$1:${CLASSPATH}"
    else
      CLASSPATH+=:$1
    fi
  fi
  return 0
}

function hadoop_add_colonpath
{
  # two params:
  # $1 = directory, file, wildcard, whatever to add
  # $2 = before or after, which determines where in the
  #      classpath this object should go
  # return 0 = success
  # return 1 = failure (duplicate)
  
  # this is CLASSPATH, JLP, etc but with dedupe but no
  # other checking
  if [[ -d "${2}" ]] && [[ ":${!1}:" != *":$2:"* ]]; then
    if [[ -z "${!1}" ]]; then
      # shellcheck disable=SC2086
      eval $1="'$2'"
    elif [[ "$3" = "before" ]]; then
      # shellcheck disable=SC2086
      eval $1="'$2:${!1}'"
    else
      # shellcheck disable=SC2086
      eval $1+="'$2'"
    fi
  fi
}

function hadoop_add_javalibpath
{
  # specialized function for a common use case
  hadoop_add_colonpath JAVA_LIBRARY_PATH "$1" "$2"
}

function hadoop_add_ldlibpath
{
  # specialized function for a common use case
  hadoop_add_colonpath LD_LIBRARY_PATH "$1" "$2"
  
  # note that we export this
  export LD_LIBRARY_PATH
}

function hadoop_add_to_classpath_common
{
  
  #
  # get all of the common jars+config in the path
  #
  
  # developers
  if [[ -n "${HADOOP_ENABLE_BUILD_PATHS}" ]]; then
    hadoop_add_classpath "${HADOOP_COMMON_HOME}/hadoop-common/target/classes"
  fi
  
  if [[ -d "${HADOOP_COMMON_HOME}/${HADOOP_COMMON_DIR}/webapps" ]]; then
    hadoop_add_classpath "${HADOOP_COMMON_HOME}/${HADOOP_COMMON_DIR}"
  fi
  
  hadoop_add_classpath "${HADOOP_COMMON_HOME}/${HADOOP_COMMON_LIB_JARS_DIR}"'/*'
  hadoop_add_classpath "${HADOOP_COMMON_HOME}/${HADOOP_COMMON_DIR}"'/*'
}

function hadoop_add_to_classpath_hdfs
{
  #
  # get all of the hdfs jars+config in the path
  #
  # developers
  if [[ -n "${HADOOP_ENABLE_BUILD_PATHS}" ]]; then
    hadoop_add_classpath "${HADOOP_HDFS_HOME}/hadoop-hdfs/target/classes"
  fi
  
  # put hdfs in classpath if present
  if [[ -d "${HADOOP_HDFS_HOME}/${HDFS_DIR}/webapps" ]]; then
    hadoop_add_classpath "${HADOOP_HDFS_HOME}/${HDFS_DIR}"
  fi
  
  hadoop_add_classpath "${HADOOP_HDFS_HOME}/${HDFS_LIB_JARS_DIR}"'/*'
  hadoop_add_classpath "${HADOOP_HDFS_HOME}/${HDFS_DIR}"'/*'
}

function hadoop_add_to_classpath_yarn
{
  #
  # get all of the yarn jars+config in the path
  #
  # developers
  if [[ -n "${HADOOP_ENABLE_BUILD_PATHS}" ]]; then
    for i in yarn-api yarn-common yarn-mapreduce yarn-master-worker \
    yarn-server/yarn-server-nodemanager \
    yarn-server/yarn-server-common \
    yarn-server/yarn-server-resourcemanager; do
      hadoop_add_classpath "${HADOOP_YARN_HOME}/$i/target/classes"
    done
    
    hadoop_add_classpath "${HADOOP_YARN_HOME}/build/test/classes"
    hadoop_add_classpath "${HADOOP_YARN_HOME}/build/tools"
  fi
  
  if [[ -d "${HADOOP_YARN_HOME}/${YARN_DIR}/webapps" ]]; then
    hadoop_add_classpath "${HADOOP_YARN_HOME}/${YARN_DIR}"
  fi
  
  hadoop_add_classpath "${HADOOP_YARN_HOME}/${YARN_LIB_JARS_DIR}"'/*'
  hadoop_add_classpath  "${HADOOP_YARN_HOME}/${YARN_DIR}"'/*'
}

function hadoop_add_to_classpath_mapred
{
  #
  # get all of the mapreduce jars+config in the path
  #
  # developers
  if [[ -n "${HADOOP_ENABLE_BUILD_PATHS}" ]]; then
    hadoop_add_classpath "${HADOOP_MAPRED_HOME}/hadoop-mapreduce-client-shuffle/target/classes"
    hadoop_add_classpath "${HADOOP_MAPRED_HOME}/hadoop-mapreduce-client-common/target/classes"
    hadoop_add_classpath "${HADOOP_MAPRED_HOME}/hadoop-mapreduce-client-hs/target/classes"
    hadoop_add_classpath "${HADOOP_MAPRED_HOME}/hadoop-mapreduce-client-hs-plugins/target/classes"
    hadoop_add_classpath "${HADOOP_MAPRED_HOME}/hadoop-mapreduce-client-app/target/classes"
    hadoop_add_classpath "${HADOOP_MAPRED_HOME}/hadoop-mapreduce-client-jobclient/target/classes"
    hadoop_add_classpath "${HADOOP_MAPRED_HOME}/hadoop-mapreduce-client-core/target/classes"
  fi
  
  if [[ -d "${HADOOP_MAPRED_HOME}/${MAPRED_DIR}/webapps" ]]; then
    hadoop_add_classpath "${HADOOP_MAPRED_HOME}/${MAPRED_DIR}"
  fi
  
  hadoop_add_classpath "${HADOOP_MAPRED_HOME}/${MAPRED_LIB_JARS_DIR}"'/*'
  hadoop_add_classpath "${HADOOP_MAPRED_HOME}/${MAPRED_DIR}"'/*'
}


function hadoop_add_to_classpath_userpath
{
  # Add the user-specified HADOOP_CLASSPATH to the
  # official CLASSPATH env var if HADOOP_USE_CLIENT_CLASSLOADER
  # is not set.
  # Add it first or last depending on if user has
  # set env-var HADOOP_USER_CLASSPATH_FIRST
  # we'll also dedupe it, because we're cool like that.
  #
  local c
  local array
  local i
  local j
  let c=0
  
  if [[ -n "${HADOOP_CLASSPATH}" ]]; then
    # I wonder if Java runs on VMS.
    for i in $(echo "${HADOOP_CLASSPATH}" | tr : '\n'); do
      array[$c]=$i
      let c+=1
    done
    let j=c-1
    
    if [[ -z "${HADOOP_USE_CLIENT_CLASSLOADER}" ]]; then
      if [[ -z "${HADOOP_USER_CLASSPATH_FIRST}" ]]; then
        for ((i=j; i>=0; i--)); do
          hadoop_add_classpath "${array[$i]}" before
        done
      else
        for ((i=0; i<=j; i++)); do
          hadoop_add_classpath "${array[$i]}" after
        done
      fi
    fi
  fi
}

function hadoop_os_tricks
{
  local bindv6only

  # some OSes have special needs. here's some out of the box
  # examples for OS X and Linux. Vendors, replace this with your special sauce.
  case ${HADOOP_OS_TYPE} in
    Darwin)
      if [[ -x /usr/libexec/java_home ]]; then
        export JAVA_HOME="$(/usr/libexec/java_home)"
      else
        export JAVA_HOME=/Library/Java/Home
      fi
    ;;
    Linux)
      bindv6only=$(/sbin/sysctl -n net.ipv6.bindv6only 2> /dev/null)

      # NOTE! HADOOP_ALLOW_IPV6 is a developer hook.  We leave it
      # undocumented in hadoop-env.sh because we don't want users to
      # shoot themselves in the foot while devs make IPv6 work.
      if [[ -n "${bindv6only}" ]] && 
         [[ "${bindv6only}" -eq "1" ]] && 
         [[ "${HADOOP_ALLOW_IPV6}" != "yes" ]]; then
        hadoop_error "ERROR: \"net.ipv6.bindv6only\" is set to 1 "
        hadoop_error "ERROR: Hadoop networking could be broken. Aborting."
        hadoop_error "ERROR: For more info: http://wiki.apache.org/hadoop/HadoopIPv6"
        exit 1
      fi
      # Newer versions of glibc use an arena memory allocator that
      # causes virtual # memory usage to explode. This interacts badly
      # with the many threads that we use in Hadoop. Tune the variable
      # down to prevent vmem explosion.
      export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}
    ;;
  esac
}

function hadoop_java_setup
{
  # Bail if we did not detect it
  if [[ -z "${JAVA_HOME}" ]]; then
    hadoop_error "ERROR: JAVA_HOME is not set and could not be found."
    exit 1
  fi
  
  if [[ ! -d "${JAVA_HOME}" ]]; then
    hadoop_error "ERROR: JAVA_HOME ${JAVA_HOME} does not exist."
    exit 1
  fi
  
  JAVA="${JAVA_HOME}/bin/java"
  
  if [[ ! -x "$JAVA" ]]; then
    hadoop_error "ERROR: $JAVA is not executable."
    exit 1
  fi
  # shellcheck disable=SC2034
  JAVA_HEAP_MAX=-Xmx1g
  HADOOP_HEAPSIZE=${HADOOP_HEAPSIZE:-1024}
  
  # check envvars which might override default args
  if [[ -n "$HADOOP_HEAPSIZE" ]]; then
    # shellcheck disable=SC2034
    JAVA_HEAP_MAX="-Xmx${HADOOP_HEAPSIZE}m"
  fi
}


function hadoop_finalize_libpaths
{
  if [[ -n "${JAVA_LIBRARY_PATH}" ]]; then
    hadoop_add_param HADOOP_OPTS java.library.path \
    "-Djava.library.path=${JAVA_LIBRARY_PATH}"
    export LD_LIBRARY_PATH
  fi
}

#
# fill in any last minute options that might not have been defined yet
#
# Note that we are replacing ' ' with '\ ' so that directories with
# spaces work correctly when run exec blah
#
function hadoop_finalize_hadoop_opts
{
  hadoop_add_param HADOOP_OPTS hadoop.log.dir "-Dhadoop.log.dir=${HADOOP_LOG_DIR/ /\ }"
  hadoop_add_param HADOOP_OPTS hadoop.log.file "-Dhadoop.log.file=${HADOOP_LOGFILE/ /\ }"
  hadoop_add_param HADOOP_OPTS hadoop.home.dir "-Dhadoop.home.dir=${HADOOP_PREFIX/ /\ }"
  hadoop_add_param HADOOP_OPTS hadoop.id.str "-Dhadoop.id.str=${HADOOP_IDENT_STRING/ /\ }"
  hadoop_add_param HADOOP_OPTS hadoop.root.logger "-Dhadoop.root.logger=${HADOOP_ROOT_LOGGER}"
  hadoop_add_param HADOOP_OPTS hadoop.policy.file "-Dhadoop.policy.file=${HADOOP_POLICYFILE/ /\ }"
  hadoop_add_param HADOOP_OPTS hadoop.security.logger "-Dhadoop.security.logger=${HADOOP_SECURITY_LOGGER}"
}

function hadoop_finalize_classpath
{
  
  # we want the HADOOP_CONF_DIR at the end
  # according to oom, it gives a 2% perf boost
  hadoop_add_classpath "${HADOOP_CONF_DIR}" after
  
  # user classpath gets added at the last minute. this allows
  # override of CONF dirs and more
  hadoop_add_to_classpath_userpath
}

function hadoop_finalize
{
  # user classpath gets added at the last minute. this allows
  # override of CONF dirs and more
  hadoop_finalize_classpath
  hadoop_finalize_libpaths
  hadoop_finalize_hadoop_opts
}

function hadoop_exit_with_usage
{
  # NOTE: This function is not user replaceable.

  local exitcode=$1
  if [[ -z $exitcode ]]; then
    exitcode=1
  fi
  if declare -F hadoop_usage >/dev/null ; then
    hadoop_usage
  elif [[ -x /usr/bin/cowsay ]]; then
    /usr/bin/cowsay -f elephant "Sorry, no help available."
  else
    hadoop_error "Sorry, no help available."
  fi
  exit $exitcode
}

function hadoop_verify_secure_prereq
{
  # if you are on an OS like Illumos that has functional roles
  # and you are using pfexec, you'll probably want to change
  # this.
  
  # ${EUID} comes from the shell itself!
  if [[ "${EUID}" -ne 0 ]] || [[ -n "${HADOOP_SECURE_COMMAND}" ]]; then
    hadoop_error "ERROR: You must be a privileged in order to run a secure serice."
    return 1
  else
    return 0
  fi
}

function hadoop_setup_secure_service
{
  # need a more complicated setup? replace me!
  
  HADOOP_PID_DIR=${HADOOP_SECURE_PID_DIR}
  HADOOP_LOG_DIR=${HADOOP_SECURE_LOG_DIR}
}

function hadoop_verify_piddir
{
  if [[ -z "${HADOOP_PID_DIR}" ]]; then
    hadoop_error "No pid directory defined."
    exit 1
  fi
  if [[ ! -w "${HADOOP_PID_DIR}" ]] && [[ ! -d "${HADOOP_PID_DIR}" ]]; then
    hadoop_error "WARNING: ${HADOOP_PID_DIR} does not exist. Creating."
    mkdir -p "${HADOOP_PID_DIR}" > /dev/null 2>&1
    if [[ $? -gt 0 ]]; then
      hadoop_error "ERROR: Unable to create ${HADOOP_PID_DIR}. Aborting."
      exit 1
    fi
  fi
  touch "${HADOOP_PID_DIR}/$$" >/dev/null 2>&1
  if [[ $? -gt 0 ]]; then
    hadoop_error "ERROR: Unable to write in ${HADOOP_PID_DIR}. Aborting."
    exit 1
  fi
  rm "${HADOOP_PID_DIR}/$$" >/dev/null 2>&1
}

function hadoop_verify_logdir
{
  if [[ -z "${HADOOP_LOG_DIR}" ]]; then
    hadoop_error "No log directory defined."
    exit 1
  fi
  if [[ ! -w "${HADOOP_LOG_DIR}" ]] && [[ ! -d "${HADOOP_LOG_DIR}" ]]; then
    hadoop_error "WARNING: ${HADOOP_LOG_DIR} does not exist. Creating."
    mkdir -p "${HADOOP_LOG_DIR}" > /dev/null 2>&1
    if [[ $? -gt 0 ]]; then
      hadoop_error "ERROR: Unable to create ${HADOOP_LOG_DIR}. Aborting."
      exit 1
    fi
  fi
  touch "${HADOOP_LOG_DIR}/$$" >/dev/null 2>&1
  if [[ $? -gt 0 ]]; then
    hadoop_error "ERROR: Unable to write in ${HADOOP_LOG_DIR}. Aborting."
    exit 1
  fi
  rm "${HADOOP_LOG_DIR}/$$" >/dev/null 2>&1
}

function hadoop_status_daemon() {
  #
  # LSB 4.1.0 compatible status command (1)
  #
  # 0 = program is running
  # 1 = dead, but still a pid (2)
  # 2 = (not used by us)
  # 3 = not running
  #
  # 1 - this is not an endorsement of the LSB
  #
  # 2 - technically, the specification says /var/run/pid, so
  #     we should never return this value, but we're giving
  #     them the benefit of a doubt and returning 1 even if
  #     our pid is not in in /var/run .
  #
  
  local pidfile=$1
  shift
  
  local pid
  
  if [[ -f "${pidfile}" ]]; then
    pid=$(cat "${pidfile}")
    if ps -p "${pid}" > /dev/null 2>&1; then
      return 0
    fi
    return 1
  fi
  return 3
}

function hadoop_java_exec
{
  # run a java command.  this is used for
  # non-daemons

  local command=$1
  local class=$2
  shift 2
  # we eval this so that paths with spaces work
  #shellcheck disable=SC2086
  eval exec "$JAVA" "-Dproc_${command}" ${HADOOP_OPTS} "${class}" "$@"

}

function hadoop_start_daemon
{
  # this is our non-privileged daemon starter
  # that fires up a daemon in the *foreground*
  # so complex! so wow! much java!
  local command=$1
  local class=$2
  shift 2
  #shellcheck disable=SC2086
  eval exec "$JAVA" "-Dproc_${command}" ${HADOOP_OPTS} "${class}" "$@"
}

function hadoop_start_daemon_wrapper
{
  # this is our non-privileged daemon start
  # that fires up a daemon in the *background*
  local daemonname=$1
  local class=$2
  local pidfile=$3
  local outfile=$4
  shift 4
  
  hadoop_rotate_log "${outfile}"
  
  hadoop_start_daemon "${daemonname}" \
  "$class" "$@" >> "${outfile}" 2>&1 < /dev/null &
  #shellcheck disable=SC2086
  echo $! > "${pidfile}" 2>/dev/null
  if [[ $? -gt 0 ]]; then
    hadoop_error "ERROR:  Cannot write pid ${pidfile}."
  fi
  
  # shellcheck disable=SC2086
  renice "${HADOOP_NICENESS}" $! >/dev/null 2>&1
  if [[ $? -gt 0 ]]; then
    hadoop_error "ERROR: Cannot set priority of process $!"
  fi
  
  # shellcheck disable=SC2086
  disown $! 2>&1
  if [[ $? -gt 0 ]]; then
    hadoop_error "ERROR: Cannot disconnect process $!"
  fi
  sleep 1
  
  # capture the ulimit output
  ulimit -a >> "${outfile}" 2>&1
  
  # shellcheck disable=SC2086
  if ! ps -p $! >/dev/null 2>&1; then
    return 1
  fi
  return 0
}

function hadoop_start_secure_daemon
{
  # this is used to launch a secure daemon in the *foreground*
  #
  local daemonname=$1
  local class=$2
  
  # pid file to create for our deamon
  local daemonpidfile=$3
  
  # where to send stdout. jsvc has bad habits so this *may* be &1
  # which means you send it to stdout!
  local daemonoutfile=$4
  
  # where to send stderr.  same thing, except &2 = stderr
  local daemonerrfile=$5
  shift 5
  
  
  
  hadoop_rotate_log "${daemonoutfile}"
  hadoop_rotate_log "${daemonerrfile}"
  
  jsvc="${JSVC_HOME}/jsvc"
  if [[ ! -f "${jsvc}" ]]; then
    hadoop_error "JSVC_HOME is not set or set incorrectly. jsvc is required to run secure"
    hadoop_error "or privileged daemons. Please download and install jsvc from "
    hadoop_error "http://archive.apache.org/dist/commons/daemon/binaries/ "
    hadoop_error "and set JSVC_HOME to the directory containing the jsvc binary."
    exit 1
  fi
  
  # note that shellcheck will throw a
  # bogus for-our-use-case 2086 here.
  # it doesn't properly support multi-line situations
  
  exec "${jsvc}" \
  "-Dproc_${daemonname}" \
  -outfile "${daemonoutfile}" \
  -errfile "${daemonerrfile}" \
  -pidfile "${daemonpidfile}" \
  -nodetach \
  -user "${HADOOP_SECURE_USER}" \
  -cp "${CLASSPATH}" \
  ${HADOOP_OPTS} \
  "${class}" "$@"
}

function hadoop_start_secure_daemon_wrapper
{
  # this wraps hadoop_start_secure_daemon to take care
  # of the dirty work to launch a daemon in the background!
  local daemonname=$1
  local class=$2
  
  # same rules as hadoop_start_secure_daemon except we
  # have some additional parameters
  
  local daemonpidfile=$3
  
  local daemonoutfile=$4
  
  # the pid file of the subprocess that spawned our
  # secure launcher
  local jsvcpidfile=$5
  
  # the output of the subprocess that spawned our secure
  # launcher
  local jsvcoutfile=$6
  
  local daemonerrfile=$7
  shift 7
  
  hadoop_rotate_log "${jsvcoutfile}"
  
  hadoop_start_secure_daemon \
  "${daemonname}" \
  "${class}" \
  "${daemonpidfile}" \
  "${daemonoutfile}" \
  "${daemonerrfile}" "$@" >> "${jsvcoutfile}" 2>&1 < /dev/null &
  
  # This wrapper should only have one child.  Unlike Shawty Lo.
  #shellcheck disable=SC2086
  echo $! > "${jsvcpidfile}" 2>/dev/null
  if [[ $? -gt 0 ]]; then
    hadoop_error "ERROR:  Cannot write pid ${pidfile}."
  fi
  sleep 1
  #shellcheck disable=SC2086
  renice "${HADOOP_NICENESS}" $! >/dev/null 2>&1
  if [[ $? -gt 0 ]]; then
    hadoop_error "ERROR: Cannot set priority of process $!"
  fi
  if [[ -f "${daemonpidfile}" ]]; then
    #shellcheck disable=SC2046
    renice "${HADOOP_NICENESS}" $(cat "${daemonpidfile}") >/dev/null 2>&1
    if [[ $? -gt 0 ]]; then
      hadoop_error "ERROR: Cannot set priority of process $(cat "${daemonpidfile}")"
    fi
  fi
  #shellcheck disable=SC2086
  disown $! 2>&1
  if [[ $? -gt 0 ]]; then
    hadoop_error "ERROR: Cannot disconnect process $!"
  fi
  # capture the ulimit output
  su "${HADOOP_SECURE_USER}" -c 'bash -c "ulimit -a"' >> "${jsvcoutfile}" 2>&1
  #shellcheck disable=SC2086
  if ! ps -p $! >/dev/null 2>&1; then
    return 1
  fi
  return 0
}

function hadoop_stop_daemon
{
  local cmd=$1
  local pidfile=$2
  shift 2
  
  local pid
  
  if [[ -f "${pidfile}" ]]; then
    pid=$(cat "$pidfile")
    
    kill "${pid}" >/dev/null 2>&1
    sleep "${HADOOP_STOP_TIMEOUT}"
    if kill -0 "${pid}" > /dev/null 2>&1; then
      hadoop_error "WARNING: ${cmd} did not stop gracefully after ${HADOOP_STOP_TIMEOUT} seconds: Trying to kill with kill -9"
      kill -9 "${pid}" >/dev/null 2>&1
    fi
    if ps -p "${pid}" > /dev/null 2>&1; then
      hadoop_error "ERROR: Unable to kill ${pid}"
    else
      rm -f "${pidfile}" >/dev/null 2>&1
    fi
  fi
}


function hadoop_stop_secure_daemon
{
  local command=$1
  local daemonpidfile=$2
  local privpidfile=$3
  shift 3
  local ret
  
  hadoop_stop_daemon "${command}" "${daemonpidfile}"
  ret=$?
  rm -f "${daemonpidfile}" "${privpidfile}" 2>/dev/null
  return ${ret}
}

function hadoop_daemon_handler
{
  local daemonmode=$1
  local daemonname=$2
  local class=$3
  local pidfile=$4
  local outfile=$5
  shift 5
  
  case ${daemonmode} in
    status)
      hadoop_status_daemon "${daemon_pidfile}"
      exit $?
    ;;
    
    stop)
      hadoop_stop_daemon "${daemonname}" "${daemon_pidfile}"
      exit $?
    ;;
    
    ##COMPAT  -- older hadoops would also start daemons by default
    start|default)
      hadoop_verify_piddir
      hadoop_verify_logdir
      hadoop_status_daemon "${daemon_pidfile}"
      if [[ $? == 0  ]]; then
        hadoop_error "${daemonname} running as process $(cat "${daemon_pidfile}").  Stop it first."
        exit 1
      else
        # stale pid file, so just remove it and continue on
        rm -f "${daemon_pidfile}" >/dev/null 2>&1
      fi
      ##COMPAT  - differenticate between --daemon start and nothing
      # "nothing" shouldn't detach
      if [[ "$daemonmode" = "default" ]]; then
        hadoop_start_daemon "${daemonname}" "${class}" "$@"
      else
        hadoop_start_daemon_wrapper "${daemonname}" \
        "${class}" "${daemon_pidfile}" "${daemon_outfile}" "$@"
      fi
    ;;
  esac
}


function hadoop_secure_daemon_handler
{
  local daemonmode=$1
  local daemonname=$2
  local classname=$3
  local daemon_pidfile=$4
  local daemon_outfile=$5
  local priv_pidfile=$6
  local priv_outfile=$7
  local priv_errfile=$8
  shift 8
  
  case ${daemonmode} in
    status)
      hadoop_status_daemon "${daemon_pidfile}"
      exit $?
    ;;
    
    stop)
      hadoop_stop_secure_daemon "${daemonname}" \
      "${daemon_pidfile}" "${priv_pidfile}"
      exit $?
    ;;
    
    ##COMPAT  -- older hadoops would also start daemons by default
    start|default)
      hadoop_verify_piddir
      hadoop_verify_logdir
      hadoop_status_daemon "${daemon_pidfile}"
      if [[ $? == 0  ]]; then
        hadoop_error "${daemonname} running as process $(cat "${daemon_pidfile}").  Stop it first."
        exit 1
      else
        # stale pid file, so just remove it and continue on
        rm -f "${daemon_pidfile}" >/dev/null 2>&1
      fi
      
      ##COMPAT  - differenticate between --daemon start and nothing
      # "nothing" shouldn't detach
      if [[ "${daemonmode}" = "default" ]]; then
        hadoop_start_secure_daemon "${daemonname}" "${classname}" \
        "${daemon_pidfile}" "${daemon_outfile}" \
        "${priv_errfile}"  "$@"
      else
        hadoop_start_secure_daemon_wrapper "${daemonname}" "${classname}" \
        "${daemon_pidfile}" "${daemon_outfile}" \
        "${priv_pidfile}" "${priv_outfile}" "${priv_errfile}"  "$@"
      fi
    ;;
  esac
}

