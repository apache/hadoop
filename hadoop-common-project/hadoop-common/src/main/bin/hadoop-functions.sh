#!/usr/bin/env bash
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

# we need to declare this globally as an array, which can only
# be done outside of a function
declare -a HADOOP_SUBCMD_USAGE
declare -a HADOOP_OPTION_USAGE

## @description  Print a message to stderr
## @audience     public
## @stability    stable
## @replaceable  no
## @param        string
function hadoop_error
{
  echo "$*" 1>&2
}

## @description  Print a message to stderr if --debug is turned on
## @audience     public
## @stability    stable
## @replaceable  no
## @param        string
function hadoop_debug
{
  if [[ -n "${HADOOP_SHELL_SCRIPT_DEBUG}" ]]; then
    echo "DEBUG: $*" 1>&2
  fi
}

## @description  Given a filename or dir, return the absolute version of it
## @description  This works as an alternative to readlink, which isn't
## @description  portable.
## @audience     public
## @stability    stable
## @param        fsobj
## @replaceable  no
## @return       0 success
## @return       1 failure
## @return       stdout abspath
function hadoop_abs
{
  declare obj=$1
  declare dir
  declare fn
  declare dirret

  if [[ ! -e ${obj} ]]; then
    return 1
  elif [[ -d ${obj} ]]; then
    dir=${obj}
  else
    dir=$(dirname -- "${obj}")
    fn=$(basename -- "${obj}")
    fn="/${fn}"
  fi

  dir=$(cd -P -- "${dir}" >/dev/null 2>/dev/null && pwd -P)
  dirret=$?
  if [[ ${dirret} = 0 ]]; then
    echo "${dir}${fn}"
    return 0
  fi
  return 1
}

## @description  Given variable $1 delete $2 from it
## @audience     public
## @stability    stable
## @replaceable  no
function hadoop_delete_entry
{
  if [[ ${!1} =~ \ ${2}\  ]] ; then
    hadoop_debug "Removing ${2} from ${1}"
    eval "${1}"=\""${!1// ${2} }"\"
  fi
}

## @description  Given variable $1 add $2 to it
## @audience     public
## @stability    stable
## @replaceable  no
function hadoop_add_entry
{
  if [[ ! ${!1} =~ \ ${2}\  ]] ; then
    hadoop_debug "Adding ${2} to ${1}"
    #shellcheck disable=SC2140
    eval "${1}"=\""${!1} ${2} "\"
  fi
}

## @description  Given variable $1 determine if $2 is in it
## @audience     public
## @stability    stable
## @replaceable  no
## @return       0 = yes, 1 = no
function hadoop_verify_entry
{
  # this unfortunately can't really be tested by bats. :(
  # so if this changes, be aware that unit tests effectively
  # do this function in them
  [[ ${!1} =~ \ ${2}\  ]]
}

## @description  Check if we are running with privilege
## @description  by default, this implementation looks for
## @description  EUID=0.  For OSes that have true privilege
## @description  separation, this should be something more complex
## @audience     private
## @stability    evolving
## @replaceable  yes
## @return       1 = no priv
## @return       0 = priv
function hadoop_privilege_check
{
  [[ "${EUID}" = 0 ]]
}

## @description  Execute a command via su when running as root
## @description  if the given user is found or exit with
## @description  failure if not.
## @description  otherwise just run it.  (This is intended to
## @description  be used by the start-*/stop-* scripts.)
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        user
## @param        commandstring
## @return       exitstatus
function hadoop_su
{
  declare user=$1
  shift
  declare idret

  if hadoop_privilege_check; then
    id -u "${user}" >/dev/null 2>&1
    idret=$?
    if [[ ${idret} != 0 ]]; then
      hadoop_error "ERROR: Refusing to run as root: ${user} account is not found. Aborting."
      return 1
    else
      su -l "${user}" -- "$@"
    fi
  else
    "$@"
  fi
}

## @description  Execute a command via su when running as root
## @description  with extra support for commands that might
## @description  legitimately start as root (e.g., datanode)
## @description  (This is intended to
## @description  be used by the start-*/stop-* scripts.)
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        user
## @param        commandstring
## @return       exitstatus
function hadoop_uservar_su
{

  ## startup matrix:
  #
  # if $EUID != 0, then exec
  # if $EUID =0 then
  #    if hdfs_subcmd_user is defined, call hadoop_su to exec
  #    if hdfs_subcmd_user is not defined, error
  #
  # For secure daemons, this means both the secure and insecure env vars need to be
  # defined.  e.g., HDFS_DATANODE_USER=root HDFS_DATANODE_SECURE_USER=hdfs
  # This function will pick up the "normal" var, switch to that user, then
  # execute the command which will then pick up the "secure" version.
  #

  declare program=$1
  declare command=$2
  shift 2

  declare uprogram
  declare ucommand
  declare uvar

  if hadoop_privilege_check; then
    uvar=$(hadoop_get_verify_uservar "${program}" "${command}")

    if [[ -n "${!uvar}" ]]; then
      hadoop_su "${!uvar}" "$@"
    else
      hadoop_error "ERROR: Attempting to launch ${program} ${command} as root"
      hadoop_error "ERROR: but there is no ${uvar} defined. Aborting launch."
      return 1
    fi
  else
    "$@"
  fi
}

## @description  Add a subcommand to the usage output
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        subcommand
## @param        subcommanddesc
function hadoop_add_subcommand
{
  local subcmd=$1
  local text=$2

  HADOOP_SUBCMD_USAGE[${HADOOP_SUBCMD_USAGE_COUNTER}]="${subcmd}@${text}"
  ((HADOOP_SUBCMD_USAGE_COUNTER=HADOOP_SUBCMD_USAGE_COUNTER+1))
}

## @description  Add an option to the usage output
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        subcommand
## @param        subcommanddesc
function hadoop_add_option
{
  local option=$1
  local text=$2

  HADOOP_OPTION_USAGE[${HADOOP_OPTION_USAGE_COUNTER}]="${option}@${text}"
  ((HADOOP_OPTION_USAGE_COUNTER=HADOOP_OPTION_USAGE_COUNTER+1))
}

## @description  Reset the usage information to blank
## @audience     private
## @stability    evolving
## @replaceable  no
function hadoop_reset_usage
{
  HADOOP_SUBCMD_USAGE=()
  HADOOP_OPTION_USAGE=()
  HADOOP_SUBCMD_USAGE_COUNTER=0
  HADOOP_OPTION_USAGE_COUNTER=0
}

## @description  Print a screen-size aware two-column output
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        array
function hadoop_generic_columnprinter
{
  declare -a input=("$@")
  declare -i i=0
  declare -i counter=0
  declare line
  declare text
  declare option
  declare giventext
  declare -i maxoptsize
  declare -i foldsize
  declare -a tmpa
  declare numcols

  if [[ -n "${COLUMNS}" ]]; then
    numcols=${COLUMNS}
  else
    numcols=$(tput cols) 2>/dev/null
  fi

  if [[ -z "${numcols}"
     || ! "${numcols}" =~ ^[0-9]+$ ]]; then
    numcols=75
  else
    ((numcols=numcols-5))
  fi

  while read -r line; do
    tmpa[${counter}]=${line}
    ((counter=counter+1))
    option=$(echo "${line}" | cut -f1 -d'@')
    if [[ ${#option} -gt ${maxoptsize} ]]; then
      maxoptsize=${#option}
    fi
  done < <(for text in "${input[@]}"; do
    echo "${text}"
  done | sort)

  i=0
  ((foldsize=numcols-maxoptsize))

  until [[ $i -eq ${#tmpa[@]} ]]; do
    option=$(echo "${tmpa[$i]}" | cut -f1 -d'@')
    giventext=$(echo "${tmpa[$i]}" | cut -f2 -d'@')

    while read -r line; do
      printf "%-${maxoptsize}s   %-s\n" "${option}" "${line}"
      option=" "
    done < <(echo "${giventext}"| fold -s -w ${foldsize})
    ((i=i+1))
  done
}

## @description  generate standard usage output
## @description  and optionally takes a class
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        execname
## @param        true|false
## @param        [text to use in place of SUBCOMMAND]
function hadoop_generate_usage
{
  local cmd=$1
  local takesclass=$2
  local subcmdtext=${3:-"SUBCOMMAND"}
  local haveoptions
  local optstring
  local havesubs
  local subcmdstring

  cmd=${cmd##*/}

  if [[ -n "${HADOOP_OPTION_USAGE_COUNTER}"
        && "${HADOOP_OPTION_USAGE_COUNTER}" -gt 0 ]]; then
    haveoptions=true
    optstring=" [OPTIONS]"
  fi

  if [[ -n "${HADOOP_SUBCMD_USAGE_COUNTER}"
        && "${HADOOP_SUBCMD_USAGE_COUNTER}" -gt 0 ]]; then
    havesubs=true
    subcmdstring=" ${subcmdtext} [${subcmdtext} OPTIONS]"
  fi

  echo "Usage: ${cmd}${optstring}${subcmdstring}"
  if [[ ${takesclass} = true ]]; then
    echo " or    ${cmd}${optstring} CLASSNAME [CLASSNAME OPTIONS]"
    echo "  where CLASSNAME is a user-provided Java class"
  fi

  if [[ "${haveoptions}" = true ]]; then
    echo ""
    echo "  OPTIONS is none or any of:"
    echo ""

    hadoop_generic_columnprinter "${HADOOP_OPTION_USAGE[@]}"
  fi

  if [[ "${havesubs}" = true ]]; then
    echo ""
    echo "  ${subcmdtext} is one of:"
    echo ""

    hadoop_generic_columnprinter "${HADOOP_SUBCMD_USAGE[@]}"
    echo ""
    echo "${subcmdtext} may print help when invoked w/o parameters or with -h."
  fi
}

## @description  Replace `oldvar` with `newvar` if `oldvar` exists.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        oldvar
## @param        newvar
function hadoop_deprecate_envvar
{
  local oldvar=$1
  local newvar=$2
  local oldval=${!oldvar}
  local newval=${!newvar}

  if [[ -n "${oldval}" ]]; then
    hadoop_error "WARNING: ${oldvar} has been replaced by ${newvar}. Using value of ${oldvar}."
    # shellcheck disable=SC2086
    eval ${newvar}=\"${oldval}\"

    # shellcheck disable=SC2086
    newval=${oldval}

    # shellcheck disable=SC2086
    eval ${newvar}=\"${newval}\"
  fi
}

## @description  Declare `var` being used and print its value.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        var
function hadoop_using_envvar
{
  local var=$1
  local val=${!var}

  if [[ -n "${val}" ]]; then
    hadoop_debug "${var} = ${val}"
  fi
}

## @description  Create the directory 'dir'.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        dir
function hadoop_mkdir
{
  local dir=$1

  if [[ ! -w "${dir}" ]] && [[ ! -d "${dir}" ]]; then
    hadoop_error "WARNING: ${dir} does not exist. Creating."
    if ! mkdir -p "${dir}"; then
      hadoop_error "ERROR: Unable to create ${dir}. Aborting."
      exit 1
    fi
  fi
}

## @description  Bootstraps the Hadoop shell environment
## @audience     private
## @stability    evolving
## @replaceable  no
function hadoop_bootstrap
{
  # the root of the Hadoop installation
  # See HADOOP-6255 for the expected directory structure layout

  if [[ -n "${DEFAULT_LIBEXEC_DIR}" ]]; then
    hadoop_error "WARNING: DEFAULT_LIBEXEC_DIR ignored. It has been replaced by HADOOP_DEFAULT_LIBEXEC_DIR."
  fi

  # By now, HADOOP_LIBEXEC_DIR should have been defined upstream
  # We can piggyback off of that to figure out where the default
  # HADOOP_FREFIX should be.  This allows us to run without
  # HADOOP_HOME ever being defined by a human! As a consequence
  # HADOOP_LIBEXEC_DIR now becomes perhaps the single most powerful
  # env var within Hadoop.
  if [[ -z "${HADOOP_LIBEXEC_DIR}" ]]; then
    hadoop_error "HADOOP_LIBEXEC_DIR is not defined.  Exiting."
    exit 1
  fi
  HADOOP_DEFAULT_PREFIX=$(cd -P -- "${HADOOP_LIBEXEC_DIR}/.." >/dev/null && pwd -P)
  HADOOP_HOME=${HADOOP_HOME:-$HADOOP_DEFAULT_PREFIX}
  export HADOOP_HOME

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
  HADOOP_TOOLS_HOME=${HADOOP_TOOLS_HOME:-${HADOOP_HOME}}
  HADOOP_TOOLS_DIR=${HADOOP_TOOLS_DIR:-"share/hadoop/tools"}
  HADOOP_TOOLS_LIB_JARS_DIR=${HADOOP_TOOLS_LIB_JARS_DIR:-"${HADOOP_TOOLS_DIR}/lib"}

  # by default, whatever we are about to run doesn't support
  # daemonization
  HADOOP_SUBCMD_SUPPORTDAEMONIZATION=false

  # by default, we have not been self-re-execed
  HADOOP_REEXECED_CMD=false

  # shellcheck disable=SC2034
  HADOOP_SUBCMD_SECURESERVICE=false

  # usage output set to zero
  hadoop_reset_usage

  export HADOOP_OS_TYPE=${HADOOP_OS_TYPE:-$(uname -s)}

  # defaults
  export HADOOP_OPTS=${HADOOP_OPTS:-"-Djava.net.preferIPv4Stack=true"}
  hadoop_debug "Initial HADOOP_OPTS=${HADOOP_OPTS}"
}

## @description  Locate Hadoop's configuration directory
## @audience     private
## @stability    evolving
## @replaceable  no
function hadoop_find_confdir
{
  local conf_dir

  # An attempt at compatibility with some Hadoop 1.x
  # installs.
  if [[ -e "${HADOOP_HOME}/conf/hadoop-env.sh" ]]; then
    conf_dir="conf"
  else
    conf_dir="etc/hadoop"
  fi
  export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-${HADOOP_HOME}/${conf_dir}}"

  hadoop_debug "HADOOP_CONF_DIR=${HADOOP_CONF_DIR}"
}

## @description  Validate ${HADOOP_CONF_DIR}
## @audience     public
## @stability    stable
## @replaceable  yes
## @return       will exit on failure conditions
function hadoop_verify_confdir
{
  # Check only log4j.properties by default.
  # --loglevel does not work without logger settings in log4j.log4j.properties.
  if [[ ! -f "${HADOOP_CONF_DIR}/log4j.properties" ]]; then
    hadoop_error "WARNING: log4j.properties is not found. HADOOP_CONF_DIR may be incomplete."
  fi
}

## @description  Import the hadoop-env.sh settings
## @audience     private
## @stability    evolving
## @replaceable  no
function hadoop_exec_hadoopenv
{
  if [[ -z "${HADOOP_ENV_PROCESSED}" ]]; then
    if [[ -f "${HADOOP_CONF_DIR}/hadoop-env.sh" ]]; then
      export HADOOP_ENV_PROCESSED=true
      # shellcheck disable=SC1090
      . "${HADOOP_CONF_DIR}/hadoop-env.sh"
    fi
  fi
}

## @description  Import the replaced functions
## @audience     private
## @stability    evolving
## @replaceable  no
function hadoop_exec_userfuncs
{
  if [[ -e "${HADOOP_CONF_DIR}/hadoop-user-functions.sh" ]]; then
    # shellcheck disable=SC1090
    . "${HADOOP_CONF_DIR}/hadoop-user-functions.sh"
  fi
}

## @description  Read the user's settings.  This provides for users to
## @description  override and/or append hadoop-env.sh. It is not meant
## @description  as a complete system override.
## @audience     private
## @stability    evolving
## @replaceable  yes
function hadoop_exec_user_hadoopenv
{
  if [[ -f "${HOME}/.hadoop-env" ]]; then
    hadoop_debug "Applying the user's .hadoop-env"
    # shellcheck disable=SC1090
    . "${HOME}/.hadoop-env"
  fi
}

## @description  Read the user's settings.  This provides for users to
## @description  run Hadoop Shell API after system bootstrap
## @audience     private
## @stability    evolving
## @replaceable  yes
function hadoop_exec_hadooprc
{
  if [[ -f "${HOME}/.hadooprc" ]]; then
    hadoop_debug "Applying the user's .hadooprc"
    # shellcheck disable=SC1090
    . "${HOME}/.hadooprc"
  fi
}

## @description  Import shellprofile.d content
## @audience     private
## @stability    evolving
## @replaceable  yes
function hadoop_import_shellprofiles
{
  local i
  local files1
  local files2

  if [[ -d "${HADOOP_LIBEXEC_DIR}/shellprofile.d" ]]; then
    files1=(${HADOOP_LIBEXEC_DIR}/shellprofile.d/*.sh)
    hadoop_debug "shellprofiles: ${files1[*]}"
  else
    hadoop_error "WARNING: ${HADOOP_LIBEXEC_DIR}/shellprofile.d doesn't exist. Functionality may not work."
  fi

  if [[ -d "${HADOOP_CONF_DIR}/shellprofile.d" ]]; then
    files2=(${HADOOP_CONF_DIR}/shellprofile.d/*.sh)
  fi

  # enable bundled shellprofiles that come
  # from hadoop-tools.  This converts the user-facing HADOOP_OPTIONAL_TOOLS
  # to the HADOOP_TOOLS_OPTIONS that the shell profiles expect.
  # See dist-tools-hooks-maker for how the example HADOOP_OPTIONAL_TOOLS
  # gets populated into hadoop-env.sh

  for i in ${HADOOP_OPTIONAL_TOOLS//,/ }; do
    hadoop_add_entry HADOOP_TOOLS_OPTIONS "${i}"
  done

  for i in "${files1[@]}" "${files2[@]}"
  do
    if [[ -n "${i}"
      && -f "${i}" ]]; then
      hadoop_debug "Profiles: importing ${i}"
      # shellcheck disable=SC1090
      . "${i}"
    fi
  done
}

## @description  Initialize the registered shell profiles
## @audience     private
## @stability    evolving
## @replaceable  yes
function hadoop_shellprofiles_init
{
  local i

  for i in ${HADOOP_SHELL_PROFILES}
  do
    if declare -F _${i}_hadoop_init >/dev/null ; then
       hadoop_debug "Profiles: ${i} init"
       # shellcheck disable=SC2086
       _${i}_hadoop_init
    fi
  done
}

## @description  Apply the shell profile classpath additions
## @audience     private
## @stability    evolving
## @replaceable  yes
function hadoop_shellprofiles_classpath
{
  local i

  for i in ${HADOOP_SHELL_PROFILES}
  do
    if declare -F _${i}_hadoop_classpath >/dev/null ; then
       hadoop_debug "Profiles: ${i} classpath"
       # shellcheck disable=SC2086
       _${i}_hadoop_classpath
    fi
  done
}

## @description  Apply the shell profile native library additions
## @audience     private
## @stability    evolving
## @replaceable  yes
function hadoop_shellprofiles_nativelib
{
  local i

  for i in ${HADOOP_SHELL_PROFILES}
  do
    if declare -F _${i}_hadoop_nativelib >/dev/null ; then
       hadoop_debug "Profiles: ${i} nativelib"
       # shellcheck disable=SC2086
       _${i}_hadoop_nativelib
    fi
  done
}

## @description  Apply the shell profile final configuration
## @audience     private
## @stability    evolving
## @replaceable  yes
function hadoop_shellprofiles_finalize
{
  local i

  for i in ${HADOOP_SHELL_PROFILES}
  do
    if declare -F _${i}_hadoop_finalize >/dev/null ; then
       hadoop_debug "Profiles: ${i} finalize"
       # shellcheck disable=SC2086
       _${i}_hadoop_finalize
    fi
  done
}

## @description  Initialize the Hadoop shell environment, now that
## @description  user settings have been imported
## @audience     private
## @stability    evolving
## @replaceable  no
function hadoop_basic_init
{
  # Some of these are also set in hadoop-env.sh.
  # we still set them here just in case hadoop-env.sh is
  # broken in some way, set up defaults, etc.
  #
  # but it is important to note that if you update these
  # you also need to update hadoop-env.sh as well!!!

  CLASSPATH=""
  hadoop_debug "Initialize CLASSPATH"

  if [[ -z "${HADOOP_COMMON_HOME}" ]] &&
  [[ -d "${HADOOP_HOME}/${HADOOP_COMMON_DIR}" ]]; then
    export HADOOP_COMMON_HOME="${HADOOP_HOME}"
  fi

  # default policy file for service-level authorization
  HADOOP_POLICYFILE=${HADOOP_POLICYFILE:-"hadoop-policy.xml"}

  # define HADOOP_HDFS_HOME
  if [[ -z "${HADOOP_HDFS_HOME}" ]] &&
     [[ -d "${HADOOP_HOME}/${HDFS_DIR}" ]]; then
    export HADOOP_HDFS_HOME="${HADOOP_HOME}"
  fi

  # define HADOOP_YARN_HOME
  if [[ -z "${HADOOP_YARN_HOME}" ]] &&
     [[ -d "${HADOOP_HOME}/${YARN_DIR}" ]]; then
    export HADOOP_YARN_HOME="${HADOOP_HOME}"
  fi

  # define HADOOP_MAPRED_HOME
  if [[ -z "${HADOOP_MAPRED_HOME}" ]] &&
     [[ -d "${HADOOP_HOME}/${MAPRED_DIR}" ]]; then
    export HADOOP_MAPRED_HOME="${HADOOP_HOME}"
  fi

  if [[ ! -d "${HADOOP_COMMON_HOME}" ]]; then
    hadoop_error "ERROR: Invalid HADOOP_COMMON_HOME"
    exit 1
  fi

  if [[ ! -d "${HADOOP_HDFS_HOME}" ]]; then
    hadoop_error "ERROR: Invalid HADOOP_HDFS_HOME"
    exit 1
  fi

  if [[ ! -d "${HADOOP_YARN_HOME}" ]]; then
    hadoop_error "ERROR: Invalid HADOOP_YARN_HOME"
    exit 1
  fi

  if [[ ! -d "${HADOOP_MAPRED_HOME}" ]]; then
    hadoop_error "ERROR: Invalid HADOOP_MAPRED_HOME"
    exit 1
  fi

  # if for some reason the shell doesn't have $USER defined
  # (e.g., ssh'd in to execute a command)
  # let's get the effective username and use that
  USER=${USER:-$(id -nu)}
  HADOOP_IDENT_STRING=${HADOOP_IDENT_STRING:-$USER}
  HADOOP_LOG_DIR=${HADOOP_LOG_DIR:-"${HADOOP_HOME}/logs"}
  HADOOP_LOGFILE=${HADOOP_LOGFILE:-hadoop.log}
  HADOOP_LOGLEVEL=${HADOOP_LOGLEVEL:-INFO}
  HADOOP_NICENESS=${HADOOP_NICENESS:-0}
  HADOOP_STOP_TIMEOUT=${HADOOP_STOP_TIMEOUT:-5}
  HADOOP_PID_DIR=${HADOOP_PID_DIR:-/tmp}
  HADOOP_ROOT_LOGGER=${HADOOP_ROOT_LOGGER:-${HADOOP_LOGLEVEL},console}
  HADOOP_DAEMON_ROOT_LOGGER=${HADOOP_DAEMON_ROOT_LOGGER:-${HADOOP_LOGLEVEL},RFA}
  HADOOP_SECURITY_LOGGER=${HADOOP_SECURITY_LOGGER:-INFO,NullAppender}
  HADOOP_SSH_OPTS=${HADOOP_SSH_OPTS-"-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10s"}
  HADOOP_SECURE_LOG_DIR=${HADOOP_SECURE_LOG_DIR:-${HADOOP_LOG_DIR}}
  HADOOP_SECURE_PID_DIR=${HADOOP_SECURE_PID_DIR:-${HADOOP_PID_DIR}}
  HADOOP_SSH_PARALLEL=${HADOOP_SSH_PARALLEL:-10}
}

## @description  Set the worker support information to the contents
## @description  of `filename`
## @audience     public
## @stability    stable
## @replaceable  no
## @param        filename
## @return       will exit if file does not exist
function hadoop_populate_workers_file
{
  local workersfile=$1
  shift
  if [[ -f "${workersfile}" ]]; then
    # shellcheck disable=2034
    HADOOP_WORKERS="${workersfile}"
  elif [[ -f "${HADOOP_CONF_DIR}/${workersfile}" ]]; then
    # shellcheck disable=2034
    HADOOP_WORKERS="${HADOOP_CONF_DIR}/${workersfile}"
  else
    hadoop_error "ERROR: Cannot find hosts file \"${workersfile}\""
    hadoop_exit_with_usage 1
  fi
}

## @description  Rotates the given `file` until `number` of
## @description  files exist.
## @audience     public
## @stability    stable
## @replaceable  no
## @param        filename
## @param        [number]
## @return       $? will contain last mv's return value
function hadoop_rotate_log
{
  #
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

## @description  Via ssh, log into `hostname` and run `command`
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        hostname
## @param        command
## @param        [...]
function hadoop_actual_ssh
{
  # we are passing this function to xargs
  # should get hostname followed by rest of command line
  local worker=$1
  shift

  # shellcheck disable=SC2086
  ssh ${HADOOP_SSH_OPTS} ${worker} $"${@// /\\ }" 2>&1 | sed "s/^/$worker: /"
}

## @description  Connect to ${HADOOP_WORKERS} or ${HADOOP_WORKER_NAMES}
## @description  and execute command.
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        command
## @param        [...]
function hadoop_connect_to_hosts
{
  # shellcheck disable=SC2124
  local params="$@"
  local worker_file
  local tmpslvnames

  #
  # ssh (or whatever) to a host
  #
  # User can specify hostnames or a file where the hostnames are (not both)
  if [[ -n "${HADOOP_WORKERS}" && -n "${HADOOP_WORKER_NAMES}" ]] ; then
    hadoop_error "ERROR: Both HADOOP_WORKERS and HADOOP_WORKER_NAMES were defined. Aborting."
    exit 1
  elif [[ -z "${HADOOP_WORKER_NAMES}" ]]; then
    if [[ -n "${HADOOP_WORKERS}" ]]; then
      worker_file=${HADOOP_WORKERS}
    elif [[ -f "${HADOOP_CONF_DIR}/workers" ]]; then
      worker_file=${HADOOP_CONF_DIR}/workers
    elif [[ -f "${HADOOP_CONF_DIR}/slaves" ]]; then
      hadoop_error "WARNING: 'slaves' file has been deprecated. Please use 'workers' file instead."
      worker_file=${HADOOP_CONF_DIR}/slaves
    fi
  fi

  # if pdsh is available, let's use it.  otherwise default
  # to a loop around ssh.  (ugh)
  if [[ -e '/usr/bin/pdsh' ]]; then
    if [[ -z "${HADOOP_WORKER_NAMES}" ]] ; then
      # if we were given a file, just let pdsh deal with it.
      # shellcheck disable=SC2086
      PDSH_SSH_ARGS_APPEND="${HADOOP_SSH_OPTS}" pdsh \
      -f "${HADOOP_SSH_PARALLEL}" -w ^"${worker_file}" $"${@// /\\ }" 2>&1
    else
      # no spaces allowed in the pdsh arg host list
      # shellcheck disable=SC2086
      tmpslvnames=$(echo ${HADOOP_WORKER_NAMES} | tr -s ' ' ,)
      PDSH_SSH_ARGS_APPEND="${HADOOP_SSH_OPTS}" pdsh \
        -f "${HADOOP_SSH_PARALLEL}" \
        -w "${tmpslvnames}" $"${@// /\\ }" 2>&1
    fi
  else
    if [[ -z "${HADOOP_WORKER_NAMES}" ]]; then
      HADOOP_WORKER_NAMES=$(sed 's/#.*$//;/^$/d' "${worker_file}")
    fi
    hadoop_connect_to_hosts_without_pdsh "${params}"
  fi
}

## @description  Connect to ${HADOOP_WORKER_NAMES} and execute command
## @description  under the environment which does not support pdsh.
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        command
## @param        [...]
function hadoop_connect_to_hosts_without_pdsh
{
  # shellcheck disable=SC2124
  local params="$@"
  local workers=(${HADOOP_WORKER_NAMES})
  for (( i = 0; i < ${#workers[@]}; i++ ))
  do
    if (( i != 0 && i % HADOOP_SSH_PARALLEL == 0 )); then
      wait
    fi
    # shellcheck disable=SC2086
    hadoop_actual_ssh "${workers[$i]}" ${params} &
  done
  wait
}

## @description  Utility routine to handle --workers mode
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        commandarray
function hadoop_common_worker_mode_execute
{
  #
  # input should be the command line as given by the user
  # in the form of an array
  #
  local argv=("$@")

  # if --workers is still on the command line, remove it
  # to prevent loops
  # Also remove --hostnames and --hosts along with arg values
  local argsSize=${#argv[@]};
  for (( i = 0; i < argsSize; i++ ))
  do
    if [[ "${argv[$i]}" =~ ^--workers$ ]]; then
      unset argv[$i]
    elif [[ "${argv[$i]}" =~ ^--hostnames$ ]] ||
      [[ "${argv[$i]}" =~ ^--hosts$ ]]; then
      unset argv[$i];
      let i++;
      unset argv[$i];
    fi
  done
  if [[ ${QATESTMODE} = true ]]; then
    echo "${argv[@]}"
    return
  fi
  hadoop_connect_to_hosts -- "${argv[@]}"
}

## @description  Verify that a shell command was passed a valid
## @description  class name
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        classname
## @return       0 = success
## @return       1 = failure w/user message
function hadoop_validate_classname
{
  local class=$1
  shift 1

  if [[ ! ${class} =~ \. ]]; then
    # assuming the arg is typo of command if it does not conatain ".".
    # class belonging to no package is not allowed as a result.
    hadoop_error "ERROR: ${class} is not COMMAND nor fully qualified CLASSNAME."
    return 1
  fi
  return 0
}

## @description  Append the `appendstring` if `checkstring` is not
## @description  present in the given `envvar`
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        envvar
## @param        checkstring
## @param        appendstring
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
    #shellcheck disable=SC2140
    eval "$1"="'${!1} $3'"
    if [[ ${!1:0:1} = ' ' ]]; then
      #shellcheck disable=SC2140
      eval "$1"="'${!1# }'"
    fi
    hadoop_debug "$1 accepted $3"
  else
    hadoop_debug "$1 declined $3"
  fi
}

## @description  Register the given `shellprofile` to the Hadoop
## @description  shell subsystem
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        shellprofile
function hadoop_add_profile
{
  # shellcheck disable=SC2086
  hadoop_add_param HADOOP_SHELL_PROFILES $1 $1
}

## @description  Add a file system object (directory, file,
## @description  wildcard, ...) to the classpath. Optionally provide
## @description  a hint as to where in the classpath it should go.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        object
## @param        [before|after]
## @return       0 = success (added or duplicate)
## @return       1 = failure (doesn't exist or some other reason)
function hadoop_add_classpath
{
  # However, with classpath (& JLP), we can do dedupe
  # along with some sanity checking (e.g., missing directories)
  # since we have a better idea of what is legal
  #
  # for wildcard at end, we can
  # at least check the dir exists
  if [[ $1 =~ ^.*\*$ ]]; then
    local mp
    mp=$(dirname "$1")
    if [[ ! -d "${mp}" ]]; then
      hadoop_debug "Rejected CLASSPATH: $1 (not a dir)"
      return 1
    fi

    # no wildcard in the middle, so check existence
    # (doesn't matter *what* it is)
  elif [[ ! $1 =~ ^.*\*.*$ ]] && [[ ! -e "$1" ]]; then
    hadoop_debug "Rejected CLASSPATH: $1 (does not exist)"
    return 1
  fi
  if [[ -z "${CLASSPATH}" ]]; then
    CLASSPATH=$1
    hadoop_debug "Initial CLASSPATH=$1"
  elif [[ ":${CLASSPATH}:" != *":$1:"* ]]; then
    if [[ "$2" = "before" ]]; then
      CLASSPATH="$1:${CLASSPATH}"
      hadoop_debug "Prepend CLASSPATH: $1"
    else
      CLASSPATH+=:$1
      hadoop_debug "Append CLASSPATH: $1"
    fi
  else
    hadoop_debug "Dupe CLASSPATH: $1"
  fi
  return 0
}

## @description  Add a file system object (directory, file,
## @description  wildcard, ...) to the colonpath.  Optionally provide
## @description  a hint as to where in the colonpath it should go.
## @description  Prior to adding, objects are checked for duplication
## @description  and check for existence.  Many other functions use
## @description  this function as their base implementation
## @description  including `hadoop_add_javalibpath` and `hadoop_add_ldlibpath`.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        envvar
## @param        object
## @param        [before|after]
## @return       0 = success (added or duplicate)
## @return       1 = failure (doesn't exist or some other reason)
function hadoop_add_colonpath
{
  # this is CLASSPATH, JLP, etc but with dedupe but no
  # other checking
  if [[ -d "${2}" ]] && [[ ":${!1}:" != *":$2:"* ]]; then
    if [[ -z "${!1}" ]]; then
      # shellcheck disable=SC2086
      eval $1="'$2'"
      hadoop_debug "Initial colonpath($1): $2"
    elif [[ "$3" = "before" ]]; then
      # shellcheck disable=SC2086
      eval $1="'$2:${!1}'"
      hadoop_debug "Prepend colonpath($1): $2"
    else
      # shellcheck disable=SC2086
      eval $1+=":'$2'"
      hadoop_debug "Append colonpath($1): $2"
    fi
    return 0
  fi
  hadoop_debug "Rejected colonpath($1): $2"
  return 1
}

## @description  Add a file system object (directory, file,
## @description  wildcard, ...) to the Java JNI path.  Optionally
## @description  provide a hint as to where in the Java JNI path
## @description  it should go.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        object
## @param        [before|after]
## @return       0 = success (added or duplicate)
## @return       1 = failure (doesn't exist or some other reason)
function hadoop_add_javalibpath
{
  # specialized function for a common use case
  hadoop_add_colonpath JAVA_LIBRARY_PATH "$1" "$2"
}

## @description  Add a file system object (directory, file,
## @description  wildcard, ...) to the LD_LIBRARY_PATH.  Optionally
## @description  provide a hint as to where in the LD_LIBRARY_PATH
## @description  it should go.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        object
## @param        [before|after]
## @return       0 = success (added or duplicate)
## @return       1 = failure (doesn't exist or some other reason)
function hadoop_add_ldlibpath
{
  local status
  # specialized function for a common use case
  hadoop_add_colonpath LD_LIBRARY_PATH "$1" "$2"
  status=$?

  # note that we export this
  export LD_LIBRARY_PATH
  return ${status}
}

## @description  Add the common/core Hadoop components to the
## @description  environment
## @audience     private
## @stability    evolving
## @replaceable  yes
## @returns      1 on failure, may exit
## @returns      0 on success
function hadoop_add_common_to_classpath
{
  #
  # get all of the common jars+config in the path
  #

  if [[ -z "${HADOOP_COMMON_HOME}"
    || -z "${HADOOP_COMMON_DIR}"
    || -z "${HADOOP_COMMON_LIB_JARS_DIR}" ]]; then
    hadoop_debug "COMMON_HOME=${HADOOP_COMMON_HOME}"
    hadoop_debug "COMMON_DIR=${HADOOP_COMMON_DIR}"
    hadoop_debug "COMMON_LIB_JARS_DIR=${HADOOP_COMMON_LIB_JARS_DIR}"
    hadoop_error "ERROR: HADOOP_COMMON_HOME or related vars are not configured."
    exit 1
  fi

  # developers
  if [[ -n "${HADOOP_ENABLE_BUILD_PATHS}" ]]; then
    hadoop_add_classpath "${HADOOP_COMMON_HOME}/hadoop-common/target/classes"
  fi

  hadoop_add_classpath "${HADOOP_COMMON_HOME}/${HADOOP_COMMON_LIB_JARS_DIR}"'/*'
  hadoop_add_classpath "${HADOOP_COMMON_HOME}/${HADOOP_COMMON_DIR}"'/*'
}

## @description  Run libexec/tools/module.sh to add to the classpath
## @description  environment
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        module
function hadoop_add_to_classpath_tools
{
  declare module=$1

  if [[ -f "${HADOOP_LIBEXEC_DIR}/tools/${module}.sh" ]]; then
    # shellcheck disable=SC1090
    . "${HADOOP_LIBEXEC_DIR}/tools/${module}.sh"
  else
    hadoop_error "ERROR: Tools helper ${HADOOP_LIBEXEC_DIR}/tools/${module}.sh was not found."
  fi

  if declare -f hadoop_classpath_tools_${module} >/dev/null 2>&1; then
    "hadoop_classpath_tools_${module}"
  fi
}

## @description  Add the user's custom classpath settings to the
## @description  environment
## @audience     private
## @stability    evolving
## @replaceable  yes
function hadoop_add_to_classpath_userpath
{
  # Add the user-specified HADOOP_CLASSPATH to the
  # official CLASSPATH env var if HADOOP_USE_CLIENT_CLASSLOADER
  # is not set.
  # Add it first or last depending on if user has
  # set env-var HADOOP_USER_CLASSPATH_FIRST
  # we'll also dedupe it, because we're cool like that.
  #
  declare -a array
  declare -i c=0
  declare -i j
  declare -i i
  declare idx

  if [[ -n "${HADOOP_CLASSPATH}" ]]; then
    # I wonder if Java runs on VMS.
    for idx in $(echo "${HADOOP_CLASSPATH}" | tr : '\n'); do
      array[${c}]=${idx}
      ((c=c+1))
    done

    # bats gets confused by j getting set to 0
    ((j=c-1)) || ${QATESTMODE}

    if [[ -z "${HADOOP_USE_CLIENT_CLASSLOADER}" ]]; then
      if [[ -z "${HADOOP_USER_CLASSPATH_FIRST}" ]]; then
        for ((i=0; i<=j; i++)); do
          hadoop_add_classpath "${array[$i]}" after
        done
      else
        for ((i=j; i>=0; i--)); do
          hadoop_add_classpath "${array[$i]}" before
        done
      fi
    fi
  fi
}

## @description  Routine to configure any OS-specific settings.
## @audience     public
## @stability    stable
## @replaceable  yes
## @return       may exit on failure conditions
function hadoop_os_tricks
{
  local bindv6only

  HADOOP_IS_CYGWIN=false
  case ${HADOOP_OS_TYPE} in
    Darwin)
      if [[ -z "${JAVA_HOME}" ]]; then
        if [[ -x /usr/libexec/java_home ]]; then
          JAVA_HOME="$(/usr/libexec/java_home)"
          export JAVA_HOME
        else
          JAVA_HOME=/Library/Java/Home
          export JAVA_HOME
        fi
      fi
    ;;
    Linux)

      # Newer versions of glibc use an arena memory allocator that
      # causes virtual # memory usage to explode. This interacts badly
      # with the many threads that we use in Hadoop. Tune the variable
      # down to prevent vmem explosion.
      export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}
      # we put this in QA test mode off so that non-Linux can test
      if [[ "${QATESTMODE}" = true ]]; then
        return
      fi

      # NOTE! HADOOP_ALLOW_IPV6 is a developer hook.  We leave it
      # undocumented in hadoop-env.sh because we don't want users to
      # shoot themselves in the foot while devs make IPv6 work.

      bindv6only=$(/sbin/sysctl -n net.ipv6.bindv6only 2> /dev/null)

      if [[ -n "${bindv6only}" ]] &&
         [[ "${bindv6only}" -eq "1" ]] &&
         [[ "${HADOOP_ALLOW_IPV6}" != "yes" ]]; then
        hadoop_error "ERROR: \"net.ipv6.bindv6only\" is set to 1 "
        hadoop_error "ERROR: Hadoop networking could be broken. Aborting."
        hadoop_error "ERROR: For more info: http://wiki.apache.org/hadoop/HadoopIPv6"
        exit 1
      fi
    ;;
    CYGWIN*)
      # Flag that we're running on Cygwin to trigger path translation later.
      HADOOP_IS_CYGWIN=true
    ;;
  esac
}

## @description  Configure/verify ${JAVA_HOME}
## @audience     public
## @stability    stable
## @replaceable  yes
## @return       may exit on failure conditions
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
}

## @description  Finish Java JNI paths prior to execution
## @audience     private
## @stability    evolving
## @replaceable  yes
function hadoop_finalize_libpaths
{
  if [[ -n "${JAVA_LIBRARY_PATH}" ]]; then
    hadoop_translate_cygwin_path JAVA_LIBRARY_PATH
    hadoop_add_param HADOOP_OPTS java.library.path \
      "-Djava.library.path=${JAVA_LIBRARY_PATH}"
    export LD_LIBRARY_PATH
  fi
}

## @description  Finish Java heap parameters prior to execution
## @audience     private
## @stability    evolving
## @replaceable  yes
function hadoop_finalize_hadoop_heap
{
  if [[ -n "${HADOOP_HEAPSIZE_MAX}" ]]; then
    if [[ "${HADOOP_HEAPSIZE_MAX}" =~ ^[0-9]+$ ]]; then
      HADOOP_HEAPSIZE_MAX="${HADOOP_HEAPSIZE_MAX}m"
    fi
    hadoop_add_param HADOOP_OPTS Xmx "-Xmx${HADOOP_HEAPSIZE_MAX}"
  fi

  # backwards compatibility
  if [[ -n "${HADOOP_HEAPSIZE}" ]]; then
    if [[ "${HADOOP_HEAPSIZE}" =~ ^[0-9]+$ ]]; then
      HADOOP_HEAPSIZE="${HADOOP_HEAPSIZE}m"
    fi
    hadoop_add_param HADOOP_OPTS Xmx "-Xmx${HADOOP_HEAPSIZE}"
  fi

  if [[ -n "${HADOOP_HEAPSIZE_MIN}" ]]; then
    if [[ "${HADOOP_HEAPSIZE_MIN}" =~ ^[0-9]+$ ]]; then
      HADOOP_HEAPSIZE_MIN="${HADOOP_HEAPSIZE_MIN}m"
    fi
    hadoop_add_param HADOOP_OPTS Xms "-Xms${HADOOP_HEAPSIZE_MIN}"
  fi
}

## @description  Converts the contents of the variable name
## @description  `varnameref` into the equivalent Windows path.
## @description  If the second parameter is true, then `varnameref`
## @description  is treated as though it was a path list.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        varnameref
## @param        [true]
function hadoop_translate_cygwin_path
{
  if [[ "${HADOOP_IS_CYGWIN}" = "true" ]]; then
    if [[ "$2" = "true" ]]; then
      #shellcheck disable=SC2016
      eval "$1"='$(cygpath -p -w "${!1}" 2>/dev/null)'
    else
      #shellcheck disable=SC2016
      eval "$1"='$(cygpath -w "${!1}" 2>/dev/null)'
    fi
  fi
}

## @description  Adds the HADOOP_CLIENT_OPTS variable to
## @description  HADOOP_OPTS if HADOOP_SUBCMD_SUPPORTDAEMONIZATION is false
## @audience     public
## @stability    stable
## @replaceable  yes
function hadoop_add_client_opts
{
  if [[ "${HADOOP_SUBCMD_SUPPORTDAEMONIZATION}" = false
     || -z "${HADOOP_SUBCMD_SUPPORTDAEMONIZATION}" ]]; then
    hadoop_debug "Appending HADOOP_CLIENT_OPTS onto HADOOP_OPTS"
    HADOOP_OPTS="${HADOOP_OPTS} ${HADOOP_CLIENT_OPTS}"
  fi
}

## @description  Finish configuring Hadoop specific system properties
## @description  prior to executing Java
## @audience     private
## @stability    evolving
## @replaceable  yes
function hadoop_finalize_hadoop_opts
{
  hadoop_translate_cygwin_path HADOOP_LOG_DIR
  hadoop_add_param HADOOP_OPTS hadoop.log.dir "-Dhadoop.log.dir=${HADOOP_LOG_DIR}"
  hadoop_add_param HADOOP_OPTS hadoop.log.file "-Dhadoop.log.file=${HADOOP_LOGFILE}"
  hadoop_translate_cygwin_path HADOOP_HOME
  export HADOOP_HOME
  hadoop_add_param HADOOP_OPTS hadoop.home.dir "-Dhadoop.home.dir=${HADOOP_HOME}"
  hadoop_add_param HADOOP_OPTS hadoop.id.str "-Dhadoop.id.str=${HADOOP_IDENT_STRING}"
  hadoop_add_param HADOOP_OPTS hadoop.root.logger "-Dhadoop.root.logger=${HADOOP_ROOT_LOGGER}"
  hadoop_add_param HADOOP_OPTS hadoop.policy.file "-Dhadoop.policy.file=${HADOOP_POLICYFILE}"
  hadoop_add_param HADOOP_OPTS hadoop.security.logger "-Dhadoop.security.logger=${HADOOP_SECURITY_LOGGER}"
}

## @description  Finish Java classpath prior to execution
## @audience     private
## @stability    evolving
## @replaceable  yes
function hadoop_finalize_classpath
{
  hadoop_add_classpath "${HADOOP_CONF_DIR}" before

  # user classpath gets added at the last minute. this allows
  # override of CONF dirs and more
  hadoop_add_to_classpath_userpath
  hadoop_translate_cygwin_path CLASSPATH true
}

## @description  Finish Catalina configuration prior to execution
## @audience     private
## @stability    evolving
## @replaceable  yes
function hadoop_finalize_catalina_opts
{

  local prefix=${HADOOP_CATALINA_PREFIX}

  hadoop_add_param CATALINA_OPTS hadoop.home.dir "-Dhadoop.home.dir=${HADOOP_HOME}"
  if [[ -n "${JAVA_LIBRARY_PATH}" ]]; then
    hadoop_add_param CATALINA_OPTS java.library.path "-Djava.library.path=${JAVA_LIBRARY_PATH}"
  fi
  hadoop_add_param CATALINA_OPTS "${prefix}.home.dir" "-D${prefix}.home.dir=${HADOOP_HOME}"
  hadoop_add_param CATALINA_OPTS "${prefix}.config.dir" "-D${prefix}.config.dir=${HADOOP_CATALINA_CONFIG}"
  hadoop_add_param CATALINA_OPTS "${prefix}.log.dir" "-D${prefix}.log.dir=${HADOOP_CATALINA_LOG}"
  hadoop_add_param CATALINA_OPTS "${prefix}.temp.dir" "-D${prefix}.temp.dir=${HADOOP_CATALINA_TEMP}"
  hadoop_add_param CATALINA_OPTS "${prefix}.admin.port" "-D${prefix}.admin.port=${HADOOP_CATALINA_ADMIN_PORT}"
  hadoop_add_param CATALINA_OPTS "${prefix}.http.port" "-D${prefix}.http.port=${HADOOP_CATALINA_HTTP_PORT}"
  hadoop_add_param CATALINA_OPTS "${prefix}.max.threads" "-D${prefix}.max.threads=${HADOOP_CATALINA_MAX_THREADS}"
  hadoop_add_param CATALINA_OPTS "${prefix}.max.http.header.size" "-D${prefix}.max.http.header.size=${HADOOP_CATALINA_MAX_HTTP_HEADER_SIZE}"
  hadoop_add_param CATALINA_OPTS "${prefix}.ssl.keystore.file" "-D${prefix}.ssl.keystore.file=${HADOOP_CATALINA_SSL_KEYSTORE_FILE}"
}

## @description  Finish all the remaining environment settings prior
## @description  to executing Java.  This is a wrapper that calls
## @description  the other `finalize` routines.
## @audience     private
## @stability    evolving
## @replaceable  yes
function hadoop_finalize
{
  hadoop_shellprofiles_finalize

  hadoop_finalize_classpath
  hadoop_finalize_libpaths
  hadoop_finalize_hadoop_heap
  hadoop_finalize_hadoop_opts

  hadoop_translate_cygwin_path HADOOP_HOME
  hadoop_translate_cygwin_path HADOOP_CONF_DIR
  hadoop_translate_cygwin_path HADOOP_COMMON_HOME
  hadoop_translate_cygwin_path HADOOP_HDFS_HOME
  hadoop_translate_cygwin_path HADOOP_YARN_HOME
  hadoop_translate_cygwin_path HADOOP_MAPRED_HOME
}

## @description  Print usage information and exit with the passed
## @description  `exitcode`
## @audience     public
## @stability    stable
## @replaceable  no
## @param        exitcode
## @return       This function will always exit.
function hadoop_exit_with_usage
{
  local exitcode=$1
  if [[ -z $exitcode ]]; then
    exitcode=1
  fi
  # shellcheck disable=SC2034
  if declare -F hadoop_usage >/dev/null ; then
    hadoop_usage
  elif [[ -x /usr/bin/cowsay ]]; then
    /usr/bin/cowsay -f elephant "Sorry, no help available."
  else
    hadoop_error "Sorry, no help available."
  fi
  exit $exitcode
}

## @description  Verify that prerequisites have been met prior to
## @description  excuting a privileged program.
## @audience     private
## @stability    evolving
## @replaceable  yes
## @return       This routine may exit.
function hadoop_verify_secure_prereq
{
  # if you are on an OS like Illumos that has functional roles
  # and you are using pfexec, you'll probably want to change
  # this.

  if ! hadoop_privilege_check && [[ -z "${HADOOP_SECURE_COMMAND}" ]]; then
    hadoop_error "ERROR: You must be a privileged user in order to run a secure service."
    exit 1
  else
    return 0
  fi
}

## @audience     private
## @stability    evolving
## @replaceable  yes
function hadoop_setup_secure_service
{
  # need a more complicated setup? replace me!

  HADOOP_PID_DIR=${HADOOP_SECURE_PID_DIR}
  HADOOP_LOG_DIR=${HADOOP_SECURE_LOG_DIR}
}

## @audience     private
## @stability    evolving
## @replaceable  yes
function hadoop_verify_piddir
{
  if [[ -z "${HADOOP_PID_DIR}" ]]; then
    hadoop_error "No pid directory defined."
    exit 1
  fi
  hadoop_mkdir "${HADOOP_PID_DIR}"
  touch "${HADOOP_PID_DIR}/$$" >/dev/null 2>&1
  if [[ $? -gt 0 ]]; then
    hadoop_error "ERROR: Unable to write in ${HADOOP_PID_DIR}. Aborting."
    exit 1
  fi
  rm "${HADOOP_PID_DIR}/$$" >/dev/null 2>&1
}

## @audience     private
## @stability    evolving
## @replaceable  yes
function hadoop_verify_logdir
{
  if [[ -z "${HADOOP_LOG_DIR}" ]]; then
    hadoop_error "No log directory defined."
    exit 1
  fi
  hadoop_mkdir "${HADOOP_LOG_DIR}"
  touch "${HADOOP_LOG_DIR}/$$" >/dev/null 2>&1
  if [[ $? -gt 0 ]]; then
    hadoop_error "ERROR: Unable to write in ${HADOOP_LOG_DIR}. Aborting."
    exit 1
  fi
  rm "${HADOOP_LOG_DIR}/$$" >/dev/null 2>&1
}

## @description  Determine the status of the daemon referenced
## @description  by `pidfile`
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        pidfile
## @return       (mostly) LSB 4.1.0 compatible status
function hadoop_status_daemon
{
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

## @description  Execute the Java `class`, passing along any `options`.
## @description  Additionally, set the Java property -Dproc_`command`.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        command
## @param        class
## @param        [options]
function hadoop_java_exec
{
  # run a java command.  this is used for
  # non-daemons

  local command=$1
  local class=$2
  shift 2

  hadoop_debug "Final CLASSPATH: ${CLASSPATH}"
  hadoop_debug "Final HADOOP_OPTS: ${HADOOP_OPTS}"
  hadoop_debug "Final JAVA_HOME: ${JAVA_HOME}"
  hadoop_debug "java: ${JAVA}"
  hadoop_debug "Class name: ${class}"
  hadoop_debug "Command line options: $*"

  export CLASSPATH
  #shellcheck disable=SC2086
  exec "${JAVA}" "-Dproc_${command}" ${HADOOP_OPTS} "${class}" "$@"
}

## @description  Start a non-privileged daemon in the foreground.
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        command
## @param        class
## @param        pidfile
## @param        [options]
function hadoop_start_daemon
{
  # this is our non-privileged daemon starter
  # that fires up a daemon in the *foreground*
  # so complex! so wow! much java!
  local command=$1
  local class=$2
  local pidfile=$3
  shift 3

  hadoop_debug "Final CLASSPATH: ${CLASSPATH}"
  hadoop_debug "Final HADOOP_OPTS: ${HADOOP_OPTS}"
  hadoop_debug "Final JAVA_HOME: ${JAVA_HOME}"
  hadoop_debug "java: ${JAVA}"
  hadoop_debug "Class name: ${class}"
  hadoop_debug "Command line options: $*"

  # this is for the non-daemon pid creation
  #shellcheck disable=SC2086
  echo $$ > "${pidfile}" 2>/dev/null
  if [[ $? -gt 0 ]]; then
    hadoop_error "ERROR:  Cannot write ${command} pid ${pidfile}."
  fi

  export CLASSPATH
  #shellcheck disable=SC2086
  exec "${JAVA}" "-Dproc_${command}" ${HADOOP_OPTS} "${class}" "$@"
}

## @description  Start a non-privileged daemon in the background.
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        command
## @param        class
## @param        pidfile
## @param        outfile
## @param        [options]
function hadoop_start_daemon_wrapper
{
  local daemonname=$1
  local class=$2
  local pidfile=$3
  local outfile=$4
  shift 4

  local counter

  hadoop_rotate_log "${outfile}"

  hadoop_start_daemon "${daemonname}" \
    "$class" \
    "${pidfile}" \
    "$@" >> "${outfile}" 2>&1 < /dev/null &

  # we need to avoid a race condition here
  # so let's wait for the fork to finish
  # before overriding with the daemonized pid
  (( counter=0 ))
  while [[ ! -f ${pidfile} && ${counter} -le 5 ]]; do
    sleep 1
    (( counter++ ))
  done

  # this is for daemon pid creation
  #shellcheck disable=SC2086
  echo $! > "${pidfile}" 2>/dev/null
  if [[ $? -gt 0 ]]; then
    hadoop_error "ERROR:  Cannot write ${daemonname} pid ${pidfile}."
  fi

  # shellcheck disable=SC2086
  renice "${HADOOP_NICENESS}" $! >/dev/null 2>&1
  if [[ $? -gt 0 ]]; then
    hadoop_error "ERROR: Cannot set priority of ${daemonname} process $!"
  fi

  # shellcheck disable=SC2086
  disown %+ >/dev/null 2>&1
  if [[ $? -gt 0 ]]; then
    hadoop_error "ERROR: Cannot disconnect ${daemonname} process $!"
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

## @description  Start a privileged daemon in the foreground.
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        command
## @param        class
## @param        daemonpidfile
## @param        daemonoutfile
## @param        daemonerrfile
## @param        wrapperpidfile
## @param        [options]
function hadoop_start_secure_daemon
{
  # this is used to launch a secure daemon in the *foreground*
  #
  local daemonname=$1
  local class=$2

  # pid file to create for our daemon
  local daemonpidfile=$3

  # where to send stdout. jsvc has bad habits so this *may* be &1
  # which means you send it to stdout!
  local daemonoutfile=$4

  # where to send stderr.  same thing, except &2 = stderr
  local daemonerrfile=$5
  local privpidfile=$6
  shift 6

  hadoop_rotate_log "${daemonoutfile}"
  hadoop_rotate_log "${daemonerrfile}"

  # shellcheck disable=SC2153
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

  hadoop_debug "Final CLASSPATH: ${CLASSPATH}"
  hadoop_debug "Final HADOOP_OPTS: ${HADOOP_OPTS}"
  hadoop_debug "Final JSVC_HOME: ${JSVC_HOME}"
  hadoop_debug "jsvc: ${jsvc}"
  hadoop_debug "Class name: ${class}"
  hadoop_debug "Command line options: $*"

  #shellcheck disable=SC2086
  echo $$ > "${privpidfile}" 2>/dev/null
  if [[ $? -gt 0 ]]; then
    hadoop_error "ERROR:  Cannot write ${daemonname} pid ${privpidfile}."
  fi

  # shellcheck disable=SC2086
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

## @description  Start a privileged daemon in the background.
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        command
## @param        class
## @param        daemonpidfile
## @param        daemonoutfile
## @param        wrapperpidfile
## @param        warpperoutfile
## @param        daemonerrfile
## @param        [options]
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

  local counter

  hadoop_rotate_log "${jsvcoutfile}"

  hadoop_start_secure_daemon \
    "${daemonname}" \
    "${class}" \
    "${daemonpidfile}" \
    "${daemonoutfile}" \
    "${daemonerrfile}" \
    "${jsvcpidfile}"  "$@" >> "${jsvcoutfile}" 2>&1 < /dev/null &

  # we need to avoid a race condition here
  # so let's wait for the fork to finish
  # before overriding with the daemonized pid
  (( counter=0 ))
  while [[ ! -f ${daemonpidfile} && ${counter} -le 5 ]]; do
    sleep 1
    (( counter++ ))
  done

  # this is for the daemon pid creation
  #shellcheck disable=SC2086
  echo $! > "${jsvcpidfile}" 2>/dev/null
  if [[ $? -gt 0 ]]; then
    hadoop_error "ERROR:  Cannot write ${daemonname} pid ${daemonpidfile}."
  fi

  sleep 1
  #shellcheck disable=SC2086
  renice "${HADOOP_NICENESS}" $! >/dev/null 2>&1
  if [[ $? -gt 0 ]]; then
    hadoop_error "ERROR: Cannot set priority of ${daemonname} process $!"
  fi
  if [[ -f "${daemonpidfile}" ]]; then
    #shellcheck disable=SC2046
    renice "${HADOOP_NICENESS}" $(cat "${daemonpidfile}" 2>/dev/null) >/dev/null 2>&1
    if [[ $? -gt 0 ]]; then
      hadoop_error "ERROR: Cannot set priority of ${daemonname} process $(cat "${daemonpidfile}" 2>/dev/null)"
    fi
  fi
  #shellcheck disable=SC2046
  disown %+ >/dev/null 2>&1
  if [[ $? -gt 0 ]]; then
    hadoop_error "ERROR: Cannot disconnect ${daemonname} process $!"
  fi
  # capture the ulimit output
  su "${HADOOP_SECURE_USER}" -c 'bash -c "ulimit -a"' >> "${jsvcoutfile}" 2>&1
  #shellcheck disable=SC2086
  if ! ps -p $! >/dev/null 2>&1; then
    return 1
  fi
  return 0
}

## @description  Stop the non-privileged `command` daemon with that
## @description  that is running at `pidfile`.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        command
## @param        pidfile
function hadoop_stop_daemon
{
  local cmd=$1
  local pidfile=$2
  shift 2

  local pid
  local cur_pid

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
      cur_pid=$(cat "$pidfile")
      if [[ "${pid}" = "${cur_pid}" ]]; then
        rm -f "${pidfile}" >/dev/null 2>&1
      else
        hadoop_error "WARNING: pid has changed for ${cmd}, skip deleting pid file"
      fi
    fi
  fi
}

## @description  Stop the privileged `command` daemon with that
## @description  that is running at `daemonpidfile` and launched with
## @description  the wrapper at `wrapperpidfile`.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        command
## @param        daemonpidfile
## @param        wrapperpidfile
function hadoop_stop_secure_daemon
{
  local command=$1
  local daemonpidfile=$2
  local privpidfile=$3
  shift 3
  local ret

  local daemon_pid
  local priv_pid
  local cur_daemon_pid
  local cur_priv_pid

  daemon_pid=$(cat "$daemonpidfile")
  priv_pid=$(cat "$privpidfile")

  hadoop_stop_daemon "${command}" "${daemonpidfile}"
  ret=$?

  cur_daemon_pid=$(cat "$daemonpidfile")
  cur_priv_pid=$(cat "$privpidfile")

  if [[ "${daemon_pid}" = "${cur_daemon_pid}" ]]; then
    rm -f "${daemonpidfile}" >/dev/null 2>&1
  else
    hadoop_error "WARNING: daemon pid has changed for ${command}, skip deleting daemon pid file"
  fi

  if [[ "${priv_pid}" = "${cur_priv_pid}" ]]; then
    rm -f "${privpidfile}" >/dev/null 2>&1
  else
    hadoop_error "WARNING: priv pid has changed for ${command}, skip deleting priv pid file"
  fi
  return ${ret}
}

## @description  Manage a non-privileged daemon.
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        [start|stop|status|default]
## @param        command
## @param        class
## @param        daemonpidfile
## @param        daemonoutfile
## @param        [options]
function hadoop_daemon_handler
{
  local daemonmode=$1
  local daemonname=$2
  local class=$3
  local daemon_pidfile=$4
  local daemon_outfile=$5
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
        hadoop_error "${daemonname} is running as process $(cat "${daemon_pidfile}").  Stop it first."
        exit 1
      else
        # stale pid file, so just remove it and continue on
        rm -f "${daemon_pidfile}" >/dev/null 2>&1
      fi
      ##COMPAT  - differenticate between --daemon start and nothing
      # "nothing" shouldn't detach
      if [[ "$daemonmode" = "default" ]]; then
        hadoop_start_daemon "${daemonname}" "${class}" "${daemon_pidfile}" "$@"
      else
        hadoop_start_daemon_wrapper "${daemonname}" \
        "${class}" "${daemon_pidfile}" "${daemon_outfile}" "$@"
      fi
    ;;
  esac
}

## @description  Manage a privileged daemon.
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        [start|stop|status|default]
## @param        command
## @param        class
## @param        daemonpidfile
## @param        daemonoutfile
## @param        wrapperpidfile
## @param        wrapperoutfile
## @param        wrappererrfile
## @param        [options]
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
        hadoop_error "${daemonname} is running as process $(cat "${daemon_pidfile}").  Stop it first."
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
        "${priv_errfile}" "${priv_pidfile}" "$@"
      else
        hadoop_start_secure_daemon_wrapper "${daemonname}" "${classname}" \
        "${daemon_pidfile}" "${daemon_outfile}" \
        "${priv_pidfile}" "${priv_outfile}" "${priv_errfile}"  "$@"
      fi
    ;;
  esac
}

## @description  Get the environment variable used to validate users
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        subcommand
## @return       string
function hadoop_get_verify_uservar
{
  declare program=$1
  declare command=$2
  declare uprogram
  declare ucommand

  if [[ -z "${BASH_VERSINFO[0]}" ]] \
     || [[ "${BASH_VERSINFO[0]}" -lt 4 ]]; then
    uprogram=$(echo "${program}" | tr '[:lower:]' '[:upper:]')
    ucommand=$(echo "${command}" | tr '[:lower:]' '[:upper:]')
  else
    uprogram=${program^^}
    ucommand=${command^^}
  fi

  echo "${uprogram}_${ucommand}_USER"
}

## @description  Verify that ${USER} is allowed to execute the
## @description  given subcommand.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        command
## @param        subcommand
## @return       return 0 on success
## @return       exit 1 on failure
function hadoop_verify_user
{
  declare program=$1
  declare command=$2
  declare uvar

  uvar=$(hadoop_get_verify_uservar "${program}" "${command}")

  if [[ -n ${!uvar} ]]; then
    if [[ ${!uvar} !=  "${USER}" ]]; then
      hadoop_error "ERROR: ${command} can only be executed by ${!uvar}."
      exit 1
    fi
  fi
  return 0
}

## @description  Verify that ${USER} is allowed to execute the
## @description  given subcommand.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        subcommand
## @return       1 on no re-exec needed
## @return       0 on need to re-exec
function hadoop_need_reexec
{
  declare program=$1
  declare command=$2
  declare uvar

  # we've already been re-execed, bail

  if [[ "${HADOOP_REEXECED_CMD}" = true ]]; then
    return 1
  fi

  # if we have privilege, and the _USER is defined, and _USER is
  # set to someone who isn't us, then yes, we should re-exec.
  # otherwise no, don't re-exec and let the system deal with it.

  if hadoop_privilege_check; then
    uvar=$(hadoop_get_verify_uservar "${program}" "${command}")
    if [[ -n ${!uvar} ]]; then
      if [[ ${!uvar} !=  "${USER}" ]]; then
        return 0
      fi
    fi
  fi
  return 1
}

## @description  Add custom (program)_(command)_OPTS to HADOOP_OPTS.
## @description  Also handles the deprecated cases from pre-3.x.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        program
## @param        subcommand
## @return       will exit on failure conditions
function hadoop_subcommand_opts
{
  declare program=$1
  declare command=$2
  declare uvar
  declare depvar
  declare uprogram
  declare ucommand

  if [[ -z "${program}" || -z "${command}" ]]; then
    return 1
  fi

  # bash 4 and up have built-in ways to upper and lower
  # case the contents of vars.  This is faster than
  # calling tr.

  if [[ -z "${BASH_VERSINFO[0]}" ]] \
     || [[ "${BASH_VERSINFO[0]}" -lt 4 ]]; then
    uprogram=$(echo "${program}" | tr '[:lower:]' '[:upper:]')
    ucommand=$(echo "${command}" | tr '[:lower:]' '[:upper:]')
  else
    uprogram=${program^^}
    ucommand=${command^^}
  fi

  uvar="${uprogram}_${ucommand}_OPTS"

  # Let's handle all of the deprecation cases early
  # HADOOP_NAMENODE_OPTS -> HDFS_NAMENODE_OPTS

  depvar="HADOOP_${ucommand}_OPTS"

  if [[ "${depvar}" != "${uvar}" ]]; then
    if [[ -n "${!depvar}" ]]; then
      hadoop_deprecate_envvar "${depvar}" "${uvar}"
    fi
  fi

  if [[ -n ${!uvar} ]]; then
    hadoop_debug "Appending ${uvar} onto HADOOP_OPTS"
    HADOOP_OPTS="${HADOOP_OPTS} ${!uvar}"
    return 0
  fi
}

## @description  Add custom (program)_(command)_SECURE_EXTRA_OPTS to HADOOP_OPTS.
## @description  This *does not* handle the pre-3.x deprecated cases
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        program
## @param        subcommand
## @return       will exit on failure conditions
function hadoop_subcommand_secure_opts
{
  declare program=$1
  declare command=$2
  declare uvar
  declare uprogram
  declare ucommand

  if [[ -z "${program}" || -z "${command}" ]]; then
    return 1
  fi

  # bash 4 and up have built-in ways to upper and lower
  # case the contents of vars.  This is faster than
  # calling tr.

  if [[ -z "${BASH_VERSINFO[0]}" ]] \
     || [[ "${BASH_VERSINFO[0]}" -lt 4 ]]; then
    uprogram=$(echo "${program}" | tr '[:lower:]' '[:upper:]')
    ucommand=$(echo "${command}" | tr '[:lower:]' '[:upper:]')
  else
    uprogram=${program^^}
    ucommand=${command^^}
  fi

  # HDFS_DATANODE_SECURE_EXTRA_OPTS
  # HDFS_NFS3_SECURE_EXTRA_OPTS
  # ...
  uvar="${uprogram}_${ucommand}_SECURE_EXTRA_OPTS"

  if [[ -n ${!uvar} ]]; then
    hadoop_debug "Appending ${uvar} onto HADOOP_OPTS"
    HADOOP_OPTS="${HADOOP_OPTS} ${!uvar}"
    return 0
  fi
}

## @description  Perform the 'hadoop classpath', etc subcommand with the given
## @description  parameters
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        [parameters]
## @return       will print & exit with no params
function hadoop_do_classpath_subcommand
{
  if [[ "$#" -gt 1 ]]; then
    eval "$1"=org.apache.hadoop.util.Classpath
  else
    hadoop_finalize
    echo "${CLASSPATH}"
    exit 0
  fi
}

## @description  generic shell script opton parser.  sets
## @description  HADOOP_PARSE_COUNTER to set number the
## @description  caller should shift
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        [parameters, typically "$@"]
function hadoop_parse_args
{
  HADOOP_DAEMON_MODE="default"
  HADOOP_PARSE_COUNTER=0

  # not all of the options supported here are supported by all commands
  # however these are:
  hadoop_add_option "--config dir" "Hadoop config directory"
  hadoop_add_option "--debug" "turn on shell script debug mode"
  hadoop_add_option "--help" "usage information"

  while true; do
    hadoop_debug "hadoop_parse_args: processing $1"
    case $1 in
      --buildpaths)
        # shellcheck disable=SC2034
        HADOOP_ENABLE_BUILD_PATHS=true
        shift
        ((HADOOP_PARSE_COUNTER=HADOOP_PARSE_COUNTER+1))
      ;;
      --config)
        shift
        confdir=$1
        shift
        ((HADOOP_PARSE_COUNTER=HADOOP_PARSE_COUNTER+2))
        if [[ -d "${confdir}" ]]; then
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
        ((HADOOP_PARSE_COUNTER=HADOOP_PARSE_COUNTER+2))
        if [[ -z "${HADOOP_DAEMON_MODE}" || \
          ! "${HADOOP_DAEMON_MODE}" =~ ^st(art|op|atus)$ ]]; then
          hadoop_error "ERROR: --daemon must be followed by either \"start\", \"stop\", or \"status\"."
          hadoop_exit_with_usage 1
        fi
      ;;
      --debug)
        shift
        # shellcheck disable=SC2034
        HADOOP_SHELL_SCRIPT_DEBUG=true
        ((HADOOP_PARSE_COUNTER=HADOOP_PARSE_COUNTER+1))
      ;;
      --help|-help|-h|help|--h|--\?|-\?|\?)
        hadoop_exit_with_usage 0
      ;;
      --hostnames)
        shift
        # shellcheck disable=SC2034
        HADOOP_WORKER_NAMES="$1"
        shift
        ((HADOOP_PARSE_COUNTER=HADOOP_PARSE_COUNTER+2))
      ;;
      --hosts)
        shift
        hadoop_populate_workers_file "$1"
        shift
        ((HADOOP_PARSE_COUNTER=HADOOP_PARSE_COUNTER+2))
      ;;
      --loglevel)
        shift
        # shellcheck disable=SC2034
        HADOOP_LOGLEVEL="$1"
        shift
        ((HADOOP_PARSE_COUNTER=HADOOP_PARSE_COUNTER+2))
      ;;
      --reexec)
        shift
        if [[ "${HADOOP_REEXECED_CMD}" = true ]]; then
          hadoop_error "ERROR: re-exec fork bomb prevention: --reexec already called"
          exit 1
        fi
        HADOOP_REEXECED_CMD=true
        ((HADOOP_PARSE_COUNTER=HADOOP_PARSE_COUNTER+1))
      ;;
      --workers)
        shift
        # shellcheck disable=SC2034
        HADOOP_WORKER_MODE=true
        ((HADOOP_PARSE_COUNTER=HADOOP_PARSE_COUNTER+1))
      ;;
      *)
        break
      ;;
    esac
  done

  hadoop_debug "hadoop_parse: asking caller to skip ${HADOOP_PARSE_COUNTER}"
}

## @description  XML-escapes the characters (&'"<>) in the given parameter.
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        string
## @return       XML-escaped string
function hadoop_xml_escape
{
  sed -e 's/&/\&amp;/g' -e 's/"/\\\&quot;/g' \
    -e "s/'/\\\\\&apos;/g" -e 's/</\\\&lt;/g' -e 's/>/\\\&gt;/g' <<< "$1"
}

## @description  sed-escapes the characters (\/&) in the given parameter.
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        string
## @return       sed-escaped string
function hadoop_sed_escape
{
  sed -e 's/[\/&]/\\&/g' <<< "$1"
}
