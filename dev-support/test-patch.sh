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

# Make sure that bash version meets the pre-requisite

if [[ -z "${BASH_VERSINFO}" ]] \
   || [[ "${BASH_VERSINFO[0]}" -lt 3 ]] \
   || [[ "${BASH_VERSINFO[0]}" -eq 3 && "${BASH_VERSINFO[1]}" -lt 2 ]]; then
  echo "bash v3.2+ is required. Sorry."
  exit 1
fi

### BUILD_URL is set by Hudson if it is run by patch process

this="${BASH_SOURCE-$0}"
BINDIR=$(cd -P -- "$(dirname -- "${this}")" >/dev/null && pwd -P)
STARTINGDIR=$(pwd)
USER_PARAMS=("$@")
GLOBALTIMER=$(date +"%s")
#shellcheck disable=SC2034
QATESTMODE=false

# global arrays
declare -a MAVEN_ARGS=("--batch-mode")
declare -a ANT_ARGS=("-noinput")
declare -a TP_HEADER
declare -a TP_VOTE_TABLE
declare -a TP_TEST_TABLE
declare -a TP_FOOTER_TABLE
declare -a MODULE_STATUS
declare -a MODULE_STATUS_TIMER
declare -a MODULE_STATUS_MSG
declare -a MODULE_STATUS_LOG
declare -a MODULE

TP_HEADER_COUNTER=0
TP_VOTE_COUNTER=0
TP_TEST_COUNTER=0
TP_FOOTER_COUNTER=0

## @description  Setup the default global variables
## @audience     public
## @stability    stable
## @replaceable  no
function setup_defaults
{
  if [[ -z "${MAVEN_HOME:-}" ]]; then
    MVN=mvn
  else
    MVN=${MAVEN_HOME}/bin/mvn
  fi

  if [[ -z "${ANT_HOME:-}" ]]; then
    ANT=ant
  else
    ANT=${ANT_HOME}/bin/ant
  fi

  PROJECT_NAME=yetus
  DOCKERFILE="${BINDIR}/test-patch-docker/Dockerfile-startstub"
  HOW_TO_CONTRIBUTE="https://wiki.apache.org/hadoop/HowToContribute"
  JENKINS=false
  BASEDIR=$(pwd)
  RELOCATE_PATCH_DIR=false

  USER_PLUGIN_DIR=""
  LOAD_SYSTEM_PLUGINS=true
  ALLOWSUMMARIES=true

  DOCKERSUPPORT=false
  ECLIPSE_HOME=${ECLIPSE_HOME:-}
  BUILD_NATIVE=${BUILD_NATIVE:-true}
  PATCH_BRANCH=""
  PATCH_BRANCH_DEFAULT="master"

  # shellcheck disable=SC2034
  CHANGED_MODULES=""
  # shellcheck disable=SC2034
  CHANGED_UNFILTERED_MODULES=""
  # shellcheck disable=SC2034
  CHANGED_UNION_MODULES=""
  USER_MODULE_LIST=""
  OFFLINE=false
  CHANGED_FILES=""
  REEXECED=false
  RESETREPO=false
  ISSUE=""
  ISSUE_RE='^(YETUS)-[0-9]+$'
  TIMER=$(date +"%s")
  PATCHURL=""
  OSTYPE=$(uname -s)
  BUILDTOOL=maven
  BUGSYSTEM=jira
  TESTFORMATS=""
  JDK_TEST_LIST="javac javadoc unit"
  GITDIFFLINES="${PATCH_DIR}/gitdifflines.txt"
  GITDIFFCONTENT="${PATCH_DIR}/gitdiffcontent.txt"

  # Solaris needs POSIX, not SVID
  case ${OSTYPE} in
    SunOS)
      AWK=${AWK:-/usr/xpg4/bin/awk}
      SED=${SED:-/usr/xpg4/bin/sed}
      WGET=${WGET:-wget}
      GIT=${GIT:-git}
      GREP=${GREP:-/usr/xpg4/bin/grep}
      PATCH=${PATCH:-/usr/gnu/bin/patch}
      DIFF=${DIFF:-/usr/gnu/bin/diff}
      FILE=${FILE:-file}
    ;;
    *)
      AWK=${AWK:-awk}
      SED=${SED:-sed}
      WGET=${WGET:-wget}
      GIT=${GIT:-git}
      GREP=${GREP:-grep}
      PATCH=${PATCH:-patch}
      DIFF=${DIFF:-diff}
      FILE=${FILE:-file}
    ;;
  esac

  RESULT=0
}

## @description  Print a message to stderr
## @audience     public
## @stability    stable
## @replaceable  no
## @param        string
function yetus_error
{
  echo "$*" 1>&2
}

## @description  Print a message to stderr if --debug is turned on
## @audience     public
## @stability    stable
## @replaceable  no
## @param        string
function yetus_debug
{
  if [[ -n "${TP_SHELL_SCRIPT_DEBUG}" ]]; then
    echo "[$(date) DEBUG]: $*" 1>&2
  fi
}

## @description  Convert the given module name to a file fragment
## @audience     public
## @stability    stable
## @replaceable  no
## @param        module
function module_file_fragment
{
  local mod=$1
  if [[ ${mod} == . ]]; then
    echo root
  else
    echo "$1" | tr '/' '_' | tr '\\' '_'
  fi
}

## @description  Convert time in seconds to m + s
## @audience     public
## @stability    stable
## @replaceable  no
## @param        seconds
function clock_display
{
  local -r elapsed=$1

  if [[ ${elapsed} -lt 0 ]]; then
    echo "N/A"
  else
    printf  "%3sm %02ss" $((elapsed/60)) $((elapsed%60))
  fi
}

## @description  Activate the local timer
## @audience     public
## @stability    stable
## @replaceable  no
function start_clock
{
  yetus_debug "Start clock"
  TIMER=$(date +"%s")
}

## @description  Print the elapsed time in seconds since the start of the local timer
## @audience     public
## @stability    stable
## @replaceable  no
function stop_clock
{
  local -r stoptime=$(date +"%s")
  local -r elapsed=$((stoptime-TIMER))
  yetus_debug "Stop clock"

  echo ${elapsed}
}

## @description  Print the elapsed time in seconds since the start of the global timer
## @audience     private
## @stability    stable
## @replaceable  no
function stop_global_clock
{
  local -r stoptime=$(date +"%s")
  local -r elapsed=$((stoptime-GLOBALTIMER))
  yetus_debug "Stop global clock"

  echo ${elapsed}
}

## @description  Add time to the local timer
## @audience     public
## @stability    stable
## @replaceable  no
## @param        seconds
function offset_clock
{
  ((TIMER=TIMER-$1))
}

## @description  Add to the header of the display
## @audience     public
## @stability    stable
## @replaceable  no
## @param        string
function add_header_line
{
  TP_HEADER[${TP_HEADER_COUNTER}]="$*"
  ((TP_HEADER_COUNTER=TP_HEADER_COUNTER+1 ))
}

## @description  Add to the output table. If the first parameter is a number
## @description  that is the vote for that column and calculates the elapsed time
## @description  based upon the last start_clock().  If it the string null, then it is
## @description  a special entry that signifies extra
## @description  content for the final column.  The second parameter is the reporting
## @description  subsystem (or test) that is providing the vote.  The second parameter
## @description  is always required.  The third parameter is any extra verbage that goes
## @description  with that subsystem.
## @audience     public
## @stability    stable
## @replaceable  no
## @param        +1/0/-1/null
## @param        subsystem
## @param        string
## @return       Elapsed time display
function add_vote_table
{
  local value=$1
  local subsystem=$2
  shift 2

  local calctime
  local -r elapsed=$(stop_clock)

  yetus_debug "add_vote_table ${value} ${subsystem} ${*}"

  calctime=$(clock_display "${elapsed}")

  if [[ ${value} == "1" ]]; then
    value="+1"
  fi

  if [[ -z ${value} ]]; then
    TP_VOTE_TABLE[${TP_VOTE_COUNTER}]="|  | ${subsystem} | | ${*:-} |"
  else
    TP_VOTE_TABLE[${TP_VOTE_COUNTER}]="| ${value} | ${subsystem} | ${calctime} | $* |"
  fi
  ((TP_VOTE_COUNTER=TP_VOTE_COUNTER+1))
}

## @description  Report the JVM version of the given directory
## @stability     stable
## @audience     private
## @replaceable  yes
## @params       directory
## @returns      version
function report_jvm_version
{
  #shellcheck disable=SC2016
  "${1}/bin/java" -version 2>&1 | head -1 | ${AWK} '{print $NF}' | tr -d \"
}

## @description  Verify if a given test is multijdk
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        test
## @return       1 = yes
## @return       0 = no
function verify_multijdk_test
{
  local i=$1

  if [[ "${JDK_DIR_LIST}" == "${JAVA_HOME}" ]]; then
    yetus_debug "MultiJDK not configured."
    return 0
  fi

  if [[ ${JDK_TEST_LIST} =~ $i ]]; then
    yetus_debug "${i} is in ${JDK_TEST_LIST} and MultiJDK configured."
    return 1
  fi
  return 0
}

## @description  Absolute path the JDK_DIR_LIST and JAVA_HOME.
## @description  if JAVA_HOME is in JDK_DIR_LIST, it is positioned last
## @stability    stable
## @audience     private
## @replaceable  yes
function fullyqualifyjdks
{
  local i
  local jdkdir
  local tmplist

  JAVA_HOME=$(cd -P -- "${JAVA_HOME}" >/dev/null && pwd -P)

  for i in ${JDK_DIR_LIST}; do
    jdkdir=$(cd -P -- "${i}" >/dev/null && pwd -P)
    if [[ ${jdkdir} != "${JAVA_HOME}" ]]; then
      tmplist="${tmplist} ${jdkdir}"
    fi
  done

  JDK_DIR_LIST="${tmplist} ${JAVA_HOME}"
  JDK_DIR_LIST=${JDK_DIR_LIST/ }
}

## @description  Put the opening environment information at the bottom
## @description  of the footer table
## @stability     stable
## @audience     private
## @replaceable  yes
function prepopulate_footer
{
  # shellcheck disable=SC2016
  local javaversion
  local listofjdks
  local -r unamea=$(uname -a)
  local i

  add_footer_table "uname" "${unamea}"
  add_footer_table "Build tool" "${BUILDTOOL}"

  if [[ -n ${PERSONALITY} ]]; then
    add_footer_table "Personality" "${PERSONALITY}"
  fi

  javaversion=$(report_jvm_version "${JAVA_HOME}")
  add_footer_table "Default Java" "${javaversion}"
  if [[ -n ${JDK_DIR_LIST}
    &&  ${JDK_DIR_LIST} != "${JAVA_HOME}" ]]; then
    for i in ${JDK_DIR_LIST}; do
      javaversion=$(report_jvm_version "${i}")
      listofjdks="${listofjdks} ${i}:${javaversion}"
    done
    add_footer_table "Multi-JDK versions" "${listofjdks}"
  fi
}

## @description  Put docker stats in various tables
## @stability     stable
## @audience     private
## @replaceable  yes
function finish_docker_stats
{
  if [[ ${DOCKERMODE} == true ]]; then
    # DOCKER_VERSION is set by our creator.
    add_footer_table "Docker" "${DOCKER_VERSION}"
  fi
}

## @description  Put the max memory consumed by maven at the bottom of the table.
## @audience     private
## @stability    stable
## @replaceable  no
function finish_footer_table
{
  local maxmem

  # `sort | head` can cause a broken pipe error, but we can ignore it just like compute_gitdiff.
  # shellcheck disable=SC2016,SC2086
  maxmem=$(find "${PATCH_DIR}" -type f -exec ${AWK} 'match($0, /^\[INFO\] Final Memory: [0-9]+/)
    { print substr($0, 22, RLENGTH-21) }' {} \; | sort -nr 2>/dev/null | head -n 1)

  if [[ -n ${maxmem} ]]; then
    add_footer_table "Max memory used" "${maxmem}MB"
  fi
}

## @description  Put the final elapsed time at the bottom of the table.
## @audience     private
## @stability    stable
## @replaceable  no
function finish_vote_table
{

  local -r elapsed=$(stop_global_clock)
  local calctime

  calctime=$(clock_display "${elapsed}")

  echo ""
  echo "Total Elapsed time: ${calctime}"
  echo ""

  TP_VOTE_TABLE[${TP_VOTE_COUNTER}]="| | | ${calctime} | |"
  ((TP_VOTE_COUNTER=TP_VOTE_COUNTER+1 ))
}

## @description  Add to the footer of the display. @@BASE@@ will get replaced with the
## @description  correct location for the local filesystem in dev mode or the URL for
## @description  Jenkins mode.
## @audience     public
## @stability    stable
## @replaceable  no
## @param        subsystem
## @param        string
function add_footer_table
{
  local subsystem=$1
  shift 1

  TP_FOOTER_TABLE[${TP_FOOTER_COUNTER}]="| ${subsystem} | $* |"
  ((TP_FOOTER_COUNTER=TP_FOOTER_COUNTER+1 ))
}

## @description  Special table just for unit test failures
## @audience     public
## @stability    stable
## @replaceable  no
## @param        failurereason
## @param        testlist
function add_test_table
{
  local failure=$1
  shift 1

  TP_TEST_TABLE[${TP_TEST_COUNTER}]="| ${failure} | $* |"
  ((TP_TEST_COUNTER=TP_TEST_COUNTER+1 ))
}

## @description  Large display for the user console
## @audience     public
## @stability    stable
## @replaceable  no
## @param        string
## @return       large chunk of text
function big_console_header
{
  local text="$*"
  local spacing=$(( (75+${#text}) /2 ))
  printf "\n\n"
  echo "============================================================================"
  echo "============================================================================"
  printf "%*s\n"  ${spacing} "${text}"
  echo "============================================================================"
  echo "============================================================================"
  printf "\n\n"
}

## @description  Find the largest size of a column of an array
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       size
function findlargest
{
  local column=$1
  shift
  local a=("$@")
  local sizeofa=${#a[@]}
  local i=0
  local string
  local maxlen=0

  until [[ ${i} -eq ${sizeofa} ]]; do
    # shellcheck disable=SC2086
    string=$( echo ${a[$i]} | cut -f$((column + 1)) -d\| )
    if [[ ${#string} -gt ${maxlen} ]]; then
      maxlen=${#string}
    fi
    i=$((i+1))
  done
  echo "${maxlen}"
}

## @description  Verify that ${JAVA_HOME} is defined
## @audience     public
## @stability    stable
## @replaceable  no
## @return       1 - no JAVA_HOME
## @return       0 - JAVA_HOME defined
function find_java_home
{
  start_clock
  if [[ -z ${JAVA_HOME:-} ]]; then
    case ${OSTYPE} in
      Darwin)
        if [[ -z "${JAVA_HOME}" ]]; then
          if [[ -x /usr/libexec/java_home ]]; then
            JAVA_HOME="$(/usr/libexec/java_home)"
            export JAVA_HOME
          else
            export JAVA_HOME=/Library/Java/Home
          fi
        fi
      ;;
      *)
      ;;
    esac
  fi

  if [[ -z ${JAVA_HOME:-} ]]; then
    echo "JAVA_HOME is not defined."
    add_vote_table -1 pre-patch "JAVA_HOME is not defined."
    return 1
  fi
  return 0
}

## @description Write the contents of a file to jenkins
## @params filename
## @stability stable
## @audience public
## @returns ${JIRACLI} exit code
function write_comment
{
  local -r commentfile=${1}
  shift

  local retval=0

  if [[ ${OFFLINE} == false
     && ${JENKINS} == true ]]; then
    ${BUGSYSTEM}_write_comment "${commentfile}"
    retval=$?
  fi
  return ${retval}
}

## @description Verify that the patch directory is still in working order
## @description since bad actors on some systems wipe it out. If not,
## @description recreate it and then exit
## @audience    private
## @stability   evolving
## @replaceable yes
## @returns     may exit on failure
function verify_patchdir_still_exists
{
  local -r commentfile=/tmp/testpatch.$$.${RANDOM}
  local extra=""

  if [[ ! -d ${PATCH_DIR} ]]; then
    rm "${commentfile}" 2>/dev/null

    echo "(!) The patch artifact directory has been removed! " > "${commentfile}"
    echo "This is a fatal error for test-patch.sh.  Aborting. " >> "${commentfile}"
    echo
    cat ${commentfile}
    echo
    if [[ ${JENKINS} == true ]]; then
      if [[ -n ${NODE_NAME} ]]; then
        extra=" (node ${NODE_NAME})"
      fi
      echo "Jenkins${extra} information at ${BUILD_URL} may provide some hints. " >> "${commentfile}"

      write_comment ${commentfile}
    fi

    rm "${commentfile}"
    cleanup_and_exit ${RESULT}
  fi
}

## @description generate a list of all files and line numbers in $GITDIFFLINES that
## @description that were added/changed in the source repo.  $GITDIFFCONTENT
## @description is same file, but also includes the content of those lines
## @audience    private
## @stability   stable
## @replaceable no
function compute_gitdiff
{
  local file
  local line
  local startline
  local counter
  local numlines
  local actual
  local content
  local outfile="${PATCH_DIR}/computegitdiff.${RANDOM}"

  pushd "${BASEDIR}" >/dev/null
  ${GIT} add --all --intent-to-add
  while read line; do
    if [[ ${line} =~ ^\+\+\+ ]]; then
      file="./"$(echo "${line}" | cut -f2- -d/)
      continue
    elif [[ ${line} =~ ^@@ ]]; then
      startline=$(echo "${line}" | cut -f3 -d' ' | cut -f1 -d, | tr -d + )
      numlines=$(echo "${line}" | cut -f3 -d' ' | cut -s -f2 -d, )
      # if this is empty, then just this line
      # if it is 0, then no lines were added and this part of the patch
      # is strictly a delete
      if [[ ${numlines} == 0 ]]; then
        continue
      elif [[ -z ${numlines} ]]; then
        numlines=1
      fi
      counter=0
      # it isn't obvious, but on MOST platforms under MOST use cases,
      # this is faster than using sed, and definitely faster than using
      # awk.
      # http://unix.stackexchange.com/questions/47407/cat-line-x-to-line-y-on-a-huge-file
      # has a good discussion w/benchmarks
      #
      # note that if tail is still sending data through the pipe, but head gets enough
      # to do what was requested, head will exit, leaving tail with a broken pipe.
      # we're going to send stderr to /dev/null and ignore the error since head's
      # output is really what we're looking for
      tail -n "+${startline}" "${file}" 2>/dev/null | head -n ${numlines} > "${outfile}"
      oldifs=${IFS}
      IFS=''
      while read -r content; do
          ((actual=counter+startline))
          echo "${file}:${actual}:" >> "${GITDIFFLINES}"
          printf "%s:%s:%s\n" "${file}" "${actual}" "${content}" >> "${GITDIFFCONTENT}"
          ((counter=counter+1))
      done < "${outfile}"
      rm "${outfile}"
      IFS=${oldifs}
    fi
  done < <("${GIT}" diff --unified=0 --no-color)

  if [[ ! -f ${GITDIFFLINES} ]]; then
    touch "${GITDIFFLINES}"
  fi
  if [[ ! -f ${GITDIFFCONTENT} ]]; then
    touch "${GITDIFFCONTENT}"
  fi

  popd >/dev/null
}

## @description  Print the command to be executing to the screen. Then
## @description  run the command, sending stdout and stderr to the given filename
## @description  This will also ensure that any directories in ${BASEDIR} have
## @description  the exec bit set as a pre-exec step.
## @audience     public
## @stability    stable
## @param        filename
## @param        command
## @param        [..]
## @replaceable  no
## @returns      $?
function echo_and_redirect
{
  local logfile=$1
  shift

  verify_patchdir_still_exists

  find "${BASEDIR}" -type d -exec chmod +x {} \;
  # to the screen
  echo "cd $(pwd)"
  echo "${*} > ${logfile} 2>&1"
  # to the log
  echo "cd $(pwd)" > "${logfile}"
  echo "${*}" >> "${logfile}"
  # run the actual command
  "${@}" >> "${logfile}" 2>&1
}

## @description is a given directory relative to BASEDIR?
## @audience    public
## @stability   stable
## @replaceable yes
## @param       path
## @returns     1 - no, path
## @returns     0 - yes, path - BASEDIR
function relative_dir
{
  local p=${1#${BASEDIR}}

  if [[ ${#p} -eq ${#1} ]]; then
    echo "${p}"
    return 1
  fi
  p=${p#/}
  echo "${p}"
  return 0
}

## @description  Print the usage information
## @audience     public
## @stability    stable
## @replaceable  no
function testpatch_usage
{
  local -r up=$(echo ${PROJECT_NAME} | tr '[:lower:]' '[:upper:]')

  echo "Usage: test-patch.sh [options] patch-file | issue-number | http"
  echo
  echo "Where:"
  echo "  patch-file is a local patch file containing the changes to test"
  echo "  issue-number is a 'Patch Available' JIRA defect number (e.g. '${up}-9902') to test"
  echo "  http is an HTTP address to download the patch file"
  echo
  echo "Options:"
  echo "--basedir=<dir>        The directory to apply the patch to (default current directory)"
  echo "--branch=<ref>         Forcibly set the branch"
  echo "--branch-default=<ref> If the branch isn't forced and we don't detect one in the patch name, use this branch (default 'master')"
  #not quite working yet
  #echo "--bugsystem=<type>     The bug system in use ('jira', the default, or 'github')"
  echo "--build-native=<bool>  If true, then build native components (default 'true')"
  echo "--build-tool=<tool>    Pick which build tool to focus around (maven, ant)"
  echo "--contrib-guide=<url>  URL to point new users towards project conventions. (default: ${HOW_TO_CONTRIBUTE} )"
  echo "--debug                If set, then output some extra stuff to stderr"
  echo "--dirty-workspace      Allow the local git workspace to have uncommitted changes"
  echo "--docker               Spawn a docker container"
  echo "--dockerfile=<file>    Dockerfile fragment to use as the base"
  echo "--issue-re=<expr>      Bash regular expression to use when trying to find a jira ref in the patch name (default: \'${ISSUE_RE}\')"
  echo "--java-home=<path>     Set JAVA_HOME (In Docker mode, this should be local to the image)"
  echo "--multijdkdirs=<paths> Comma delimited lists of JDK paths to use for multi-JDK tests"
  echo "--multijdktests=<list> Comma delimited tests to use when multijdkdirs is used. (default: javac,javadoc,unit)"
  echo "--modulelist=<list>    Specify additional modules to test (comma delimited)"
  echo "--offline              Avoid connecting to the Internet"
  echo "--patch-dir=<dir>      The directory for working and output files (default '/tmp/test-patch-${PROJECT_NAME}/pid')"
  echo "--personality=<file>   The personality file to load"
  echo "--plugins=<dir>        A directory of user provided plugins. see test-patch.d for examples (default empty)"
  echo "--project=<name>       The short name for project currently using test-patch (default 'yetus')"
  echo "--resetrepo            Forcibly clean the repo"
  echo "--run-tests            Run all relevant tests below the base directory"
  echo "--skip-dirs=<list>     Skip following directories for module finding"
  echo "--skip-system-plugins  Do not load plugins from ${BINDIR}/test-patch.d"
  echo "--summarize=<bool>     Allow tests to summarize results"
  echo "--testlist=<list>      Specify which subsystem tests to use (comma delimited)"
  echo "--test-parallel=<bool> Run multiple tests in parallel (default false in developer mode, true in Jenkins mode)"
  echo "--test-threads=<int>   Number of tests to run in parallel (default defined in ${PROJECT_NAME} build)"
  echo ""
  echo "Shell binary overrides:"
  echo "--ant-cmd=<cmd>        The 'ant' command to use (default \${ANT_HOME}/bin/ant, or 'ant')"
  echo "--awk-cmd=<cmd>        The 'awk' command to use (default 'awk')"
  echo "--diff-cmd=<cmd>       The GNU-compatible 'diff' command to use (default 'diff')"
  echo "--file-cmd=<cmd>       The 'file' command to use (default 'file')"
  echo "--git-cmd=<cmd>        The 'git' command to use (default 'git')"
  echo "--grep-cmd=<cmd>       The 'grep' command to use (default 'grep')"
  echo "--mvn-cmd=<cmd>        The 'mvn' command to use (default \${MAVEN_HOME}/bin/mvn, or 'mvn')"
  echo "--patch-cmd=<cmd>      The 'patch' command to use (default 'patch')"
  echo "--sed-cmd=<cmd>        The 'sed' command to use (default 'sed')"

  echo
  echo "Jenkins-only options:"
  echo "--jenkins              Run by Jenkins (runs tests and posts results to JIRA)"
  echo "--build-url            Set the build location web page"
  echo "--eclipse-home=<path>  Eclipse home directory (default ECLIPSE_HOME environment variable)"
  echo "--mv-patch-dir         Move the patch-dir into the basedir during cleanup."
  echo "--wget-cmd=<cmd>       The 'wget' command to use (default 'wget')"

  importplugins

  for plugin in ${PLUGINS} ${BUGSYSTEMS} ${TESTFORMATS}; do
    if declare -f ${plugin}_usage >/dev/null 2>&1; then
      echo
      "${plugin}_usage"
    fi
  done
}

## @description  Interpret the command line parameters
## @audience     private
## @stability    stable
## @replaceable  no
## @params       $@
## @return       May exit on failure
function parse_args
{
  local i
  local j
  local testlist

  for i in "$@"; do
    case ${i} in
      --ant-cmd=*)
        ANT=${i#*=}
      ;;
      --awk-cmd=*)
        AWK=${i#*=}
      ;;
      --basedir=*)
        BASEDIR=${i#*=}
      ;;
      --branch=*)
        PATCH_BRANCH=${i#*=}
      ;;
      --branch-default=*)
        PATCH_BRANCH_DEFAULT=${i#*=}
      ;;
      --bugsystem=*)
        BUGSYSTEM=${i#*=}
      ;;
      --build-native=*)
        BUILD_NATIVE=${i#*=}
      ;;
      --build-tool=*)
        BUILDTOOL=${i#*=}
      ;;
      --build-url=*)
        BUILD_URL=${i#*=}
      ;;
      --contrib-guide=*)
        HOW_TO_CONTRIBUTE=${i#*=}
      ;;
      --debug)
        TP_SHELL_SCRIPT_DEBUG=true
      ;;
      --diff-cmd=*)
        DIFF=${i#*=}
      ;;
      --dirty-workspace)
        DIRTY_WORKSPACE=true
      ;;
      --docker)
        DOCKERSUPPORT=true
      ;;
      --dockerfile=*)
        DOCKERFILE=${i#*=}
      ;;
      --dockermode)
        DOCKERMODE=true
      ;;
      --eclipse-home=*)
        ECLIPSE_HOME=${i#*=}
      ;;
      --file-cmd=*)
        FILE=${i#*=}
      ;;
      --git-cmd=*)
        GIT=${i#*=}
      ;;
      --grep-cmd=*)
        GREP=${i#*=}
      ;;
      --help|-help|-h|help|--h|--\?|-\?|\?)
        testpatch_usage
        exit 0
      ;;
      --issue-re=*)
        ISSUE_RE=${i#*=}
      ;;
      --java-home=*)
        JAVA_HOME=${i#*=}
      ;;
      --jenkins)
        JENKINS=true
        TEST_PARALLEL=${TEST_PARALLEL:-true}
      ;;
      --modulelist=*)
        USER_MODULE_LIST=${i#*=}
        USER_MODULE_LIST=${USER_MODULE_LIST//,/ }
        yetus_debug "Manually forcing modules ${USER_MODULE_LIST}"
      ;;
      --multijdkdirs=*)
        JDK_DIR_LIST=${i#*=}
        JDK_DIR_LIST=${JDK_DIR_LIST//,/ }
        yetus_debug "Multi-JVM mode activated with ${JDK_DIR_LIST}"
      ;;
      --multijdktests=*)
        JDK_TEST_LIST=${i#*=}
        JDK_TEST_LIST=${JDK_TEST_LIST//,/ }
        yetus_debug "Multi-JVM test list: ${JDK_TEST_LIST}"
      ;;
      --mvn-cmd=*)
        MVN=${i#*=}
      ;;
      --mv-patch-dir)
        RELOCATE_PATCH_DIR=true;
      ;;
      --offline)
        OFFLINE=true
      ;;
      --patch-cmd=*)
        PATCH=${i#*=}
      ;;
      --patch-dir=*)
        USER_PATCH_DIR=${i#*=}
      ;;
      --personality=*)
        PERSONALITY=${i#*=}
      ;;
      --plugins=*)
        USER_PLUGIN_DIR=${i#*=}
      ;;
      --project=*)
        PROJECT_NAME=${i#*=}
      ;;
      --reexec)
        REEXECED=true
      ;;
      --resetrepo)
        RESETREPO=true
      ;;
      --run-tests)
        RUN_TESTS=true
      ;;
      --skip-dirs=*)
        MODULE_SKIPDIRS=${i#*=}
        MODULE_SKIPDIRS=${MODULE_SKIPDIRS//,/ }
        yetus_debug "Setting skipdirs to ${MODULE_SKIPDIRS}"
      ;;
      --skip-system-plugins)
        LOAD_SYSTEM_PLUGINS=false
      ;;
      --summarize=*)
        ALLOWSUMMARIES=${i#*=}
      ;;
      --testlist=*)
        testlist=${i#*=}
        testlist=${testlist//,/ }
        for j in ${testlist}; do
          yetus_debug "Manually adding patch test subsystem ${j}"
          add_test "${j}"
        done
      ;;
      --test-parallel=*)
        TEST_PARALLEL=${i#*=}
      ;;
      --test-threads=*)
        # shellcheck disable=SC2034
        TEST_THREADS=${i#*=}
      ;;
      --tpglobaltimer=*)
        GLOBALTIMER=${i#*=}
      ;;
      --tpreexectimer=*)
        REEXECLAUNCHTIMER=${i#*=}
      ;;
      --wget-cmd=*)
        WGET=${i#*=}
      ;;
      --*)
        ## PATCH_OR_ISSUE can't be a --.  So this is probably
        ## a plugin thing.
        continue
      ;;
      *)
        PATCH_OR_ISSUE=${i}
      ;;
    esac
  done

  if [[ -n ${REEXECLAUNCHTIMER} ]]; then
    TIMER=${REEXECLAUNCHTIMER};
  else
    start_clock
  fi

  if [[ ${REEXECED} == true
    && ${DOCKERMODE} == true ]]; then
    add_vote_table 0 reexec "docker + precommit patch detected."
  elif [[ ${REEXECED} == true ]]; then
    add_vote_table 0 reexec "precommit patch detected."
  elif [[ ${DOCKERMODE} == true ]]; then
    add_vote_table 0 reexec "docker mode."
  fi

  # if we requested offline, pass that to mvn
  if [[ ${OFFLINE} == "true" ]]; then
    MAVEN_ARGS=(${MAVEN_ARGS[@]} --offline)
    ANT_ARGS=(${ANT_ARGS[@]} -Doffline=)
  fi

  if [[ -z "${PATCH_OR_ISSUE}" ]]; then
    testpatch_usage
    exit 1
  fi

  # we need absolute dir for ${BASEDIR}
  cd "${STARTINGDIR}"
  BASEDIR=$(cd -P -- "${BASEDIR}" >/dev/null && pwd -P)

  if [[ -n ${USER_PATCH_DIR} ]]; then
    PATCH_DIR="${USER_PATCH_DIR}"
  else
    PATCH_DIR=/tmp/test-patch-${PROJECT_NAME}/$$
  fi

  cd "${STARTINGDIR}"
  if [[ ! -d ${PATCH_DIR} ]]; then
    mkdir -p "${PATCH_DIR}"
    if [[ $? == 0 ]] ; then
      echo "${PATCH_DIR} has been created"
    else
      echo "Unable to create ${PATCH_DIR}"
      cleanup_and_exit 1
    fi
  fi

  # we need absolute dir for PATCH_DIR
  PATCH_DIR=$(cd -P -- "${PATCH_DIR}" >/dev/null && pwd -P)

  if [[ ${JENKINS} == "true" ]]; then
    echo "Running in Jenkins mode"
    ISSUE=${PATCH_OR_ISSUE}
    RESETREPO=true
    # shellcheck disable=SC2034
    ECLIPSE_PROPERTY="-Declipse.home=${ECLIPSE_HOME}"
  else
    if [[ ${RESETREPO} == "true" ]] ; then
      echo "Running in destructive (--resetrepo) developer mode"
    else
      echo "Running in developer mode"
    fi
    JENKINS=false
  fi

  if [[ -n "${USER_PLUGIN_DIR}" ]]; then
    USER_PLUGIN_DIR=$(cd -P -- "${USER_PLUGIN_DIR}" >/dev/null && pwd -P)
  fi

  GITDIFFLINES="${PATCH_DIR}/gitdifflines.txt"
  GITDIFFCONTENT="${PATCH_DIR}/gitdiffcontent.txt"
}

## @description  Locate the build file for a given directory
## @audience     private
## @stability    stable
## @replaceable  no
## @return       directory containing the buildfile. Nothing returned if not found.
## @params       buildfile
## @params       directory
function find_buildfile_dir
{
  local buildfile=$1
  local dir=$2

  yetus_debug "Find ${buildfile} dir for: ${dir}"

  while builtin true; do
    if [[ -f "${dir}/${buildfile}" ]];then
      echo "${dir}"
      yetus_debug "Found: ${dir}"
      return 0
    elif [[ ${dir} == "." ]]; then
      yetus_debug "ERROR: ${buildfile} is not found."
      return 1
    else
      dir=$(dirname "${dir}")
    fi
  done
}

## @description  List of files that ${PATCH_DIR}/patch modifies
## @audience     private
## @stability    stable
## @replaceable  no
## @return       None; sets ${CHANGED_FILES}
function find_changed_files
{
  # get a list of all of the files that have been changed,
  # except for /dev/null (which would be present for new files).
  # Additionally, remove any a/ b/ patterns at the front of the patch filenames.
  # shellcheck disable=SC2016
  CHANGED_FILES=$(${AWK} 'function p(s){sub("^[ab]/","",s); if(s!~"^/dev/null"){print s}}
    /^diff --git /   { p($3); p($4) }
    /^(\+\+\+|---) / { p($2) }' "${PATCH_DIR}/patch" | sort -u)
}

## @description Check for directories to skip during
## @description changed module calcuation
## @audience    private
## @stability   stable
## @replaceable no
## @params      directory
## @returns     0 for use
## @returns     1 for skip
function module_skipdir
{
  local dir=${1}
  local i

  yetus_debug "Checking skipdirs for ${dir}"

  if [[ -z ${MODULE_SKIPDIRS} ]]; then
    yetus_debug "Skipping skipdirs"
    return 0
  fi

  while builtin true; do
    for i in ${MODULE_SKIPDIRS}; do
      if [[ ${dir} = "${i}" ]];then
        yetus_debug "Found a skip: ${dir}"
        return 1
      fi
    done
    if [[ ${dir} == "." ]]; then
      return 0
    else
      dir=$(dirname "${dir}")
      yetus_debug "Trying to skip: ${dir}"
    fi
  done
}

## @description  Find the modules of the build that ${PATCH_DIR}/patch modifies
## @audience     private
## @stability    stable
## @replaceable  no
## @return       None; sets ${CHANGED_MODULES} and ${CHANGED_UNFILTERED_MODULES}
function find_changed_modules
{
  local i
  local changed_dirs
  local builddirs
  local builddir
  local module
  local buildmods
  local prev_builddir
  local i=1
  local dir
  local buildfile

  case ${BUILDTOOL} in
    maven)
      buildfile=pom.xml
    ;;
    ant)
      buildfile=build.xml
    ;;
    *)
      yetus_error "ERROR: Unsupported build tool."
      output_to_console 1
      output_to_bugsystem 1
      cleanup_and_exit 1
    ;;
  esac

  changed_dirs=$(for i in ${CHANGED_FILES}; do dirname "${i}"; done | sort -u)

  # Now find all the modules that were changed
  for i in ${changed_dirs}; do

    module_skipdir "${i}"
    if [[ $? != 0 ]]; then
      continue
    fi

    builddir=$(find_buildfile_dir ${buildfile} "${i}")
    if [[ -z ${builddir} ]]; then
      yetus_error "ERROR: ${buildfile} is not found. Make sure the target is a ${BUILDTOOL}-based project."
      output_to_console 1
      output_to_bugsystem 1
      cleanup_and_exit 1
    fi
    builddirs="${builddirs} ${builddir}"
  done

  #shellcheck disable=SC2086,SC2034
  CHANGED_UNFILTERED_MODULES=$(echo ${builddirs} ${USER_MODULE_LIST} | tr ' ' '\n' | sort -u)
  #shellcheck disable=SC2086,SC2116
  CHANGED_UNFILTERED_MODULES=$(echo ${CHANGED_UNFILTERED_MODULES})


  if [[ ${BUILDTOOL} = maven ]]; then
    # Filter out modules without code
    for module in ${builddirs}; do
      ${GREP} "<packaging>pom</packaging>" "${module}/pom.xml" > /dev/null
      if [[ "$?" != 0 ]]; then
        buildmods="${buildmods} ${module}"
      fi
    done
  fi

  #shellcheck disable=SC2086,SC2034
  CHANGED_MODULES=$(echo ${buildmods} ${USER_MODULE_LIST} | tr ' ' '\n' | sort -u)

  # turn it back into a list so that anyone printing doesn't
  # generate multiline output
  #shellcheck disable=SC2086,SC2116
  CHANGED_MODULES=$(echo ${CHANGED_MODULES})

  yetus_debug "Locate the union of ${CHANGED_MODULES}"
  # shellcheck disable=SC2086
  count=$(echo ${CHANGED_MODULES} | wc -w)
  if [[ ${count} -lt 2 ]]; then
    yetus_debug "Only one entry, so keeping it ${CHANGED_MODULES}"
    # shellcheck disable=SC2034
    CHANGED_UNION_MODULES=${CHANGED_MODULES}
    return
  fi

  i=1
  while [[ ${i} -lt 100 ]]
  do
    module=$(echo "${CHANGED_MODULES}" | tr ' ' '\n' | cut -f1-${i} -d/ | uniq)
    count=$(echo "${module}" | wc -w)
    if [[ ${count} -eq 1
      && -f ${module}/${buildfile} ]]; then
      prev_builddir=${module}
    elif [[ ${count} -gt 1 ]]; then
      builddir=${prev_builddir}
      break
    fi
    ((i=i+1))
  done

  if [[ -z ${builddir} ]]; then
    builddir="."
  fi

  yetus_debug "Finding union of ${builddir}"
  builddir=$(find_buildfile_dir ${buildfile} "${builddir}" || true)

  #shellcheck disable=SC2034
  CHANGED_UNION_MODULES="${builddir}"
}

## @description  git checkout the appropriate branch to test.  Additionally, this calls
## @description  'determine_issue' and 'determine_branch' based upon the context provided
## @description  in ${PATCH_DIR} and in git after checkout.
## @audience     private
## @stability    stable
## @replaceable  no
## @return       0 on success.  May exit on failure.
function git_checkout
{
  local currentbranch
  local exemptdir

  big_console_header "Confirming git environment"

  cd "${BASEDIR}"
  if [[ ! -d .git ]]; then
    yetus_error "ERROR: ${BASEDIR} is not a git repo."
    cleanup_and_exit 1
  fi

  if [[ ${RESETREPO} == "true" ]] ; then
    ${GIT} reset --hard
    if [[ $? != 0 ]]; then
      yetus_error "ERROR: git reset is failing"
      cleanup_and_exit 1
    fi

    # if PATCH_DIR is in BASEDIR, then we don't want
    # git wiping it out.
    exemptdir=$(relative_dir "${PATCH_DIR}")
    if [[ $? == 1 ]]; then
      ${GIT} clean -xdf
    else
      # we do, however, want it emptied of all _files_.
      # we need to leave _directories_ in case we are in
      # re-exec mode (which places a directory full of stuff in it)
      yetus_debug "Exempting ${exemptdir} from clean"
      rm "${PATCH_DIR}/*" 2>/dev/null
      ${GIT} clean -xdf -e "${exemptdir}"
    fi
    if [[ $? != 0 ]]; then
      yetus_error "ERROR: git clean is failing"
      cleanup_and_exit 1
    fi

    ${GIT} checkout --force "${PATCH_BRANCH_DEFAULT}"
    if [[ $? != 0 ]]; then
      yetus_error "ERROR: git checkout --force ${PATCH_BRANCH_DEFAULT} is failing"
      cleanup_and_exit 1
    fi

    determine_branch

    # we need to explicitly fetch in case the
    # git ref hasn't been brought in tree yet
    if [[ ${OFFLINE} == false ]]; then
      ${GIT} pull --rebase
      if [[ $? != 0 ]]; then
        yetus_error "ERROR: git pull is failing"
        cleanup_and_exit 1
      fi
    fi
    # forcibly checkout this branch or git ref
    ${GIT} checkout --force "${PATCH_BRANCH}"
    if [[ $? != 0 ]]; then
      yetus_error "ERROR: git checkout ${PATCH_BRANCH} is failing"
      cleanup_and_exit 1
    fi

    # if we've selected a feature branch that has new changes
    # since our last build, we'll need to rebase to see those changes.
    if [[ ${OFFLINE} == false ]]; then
      ${GIT} pull --rebase
      if [[ $? != 0 ]]; then
        yetus_error "ERROR: git pull is failing"
        cleanup_and_exit 1
      fi
    fi

  else

    status=$(${GIT} status --porcelain)
    if [[ "${status}" != "" && -z ${DIRTY_WORKSPACE} ]] ; then
      yetus_error "ERROR: --dirty-workspace option not provided."
      yetus_error "ERROR: can't run in a workspace that contains the following modifications"
      yetus_error "${status}"
      cleanup_and_exit 1
    fi

    determine_branch

    currentbranch=$(${GIT} rev-parse --abbrev-ref HEAD)
    if [[ "${currentbranch}" != "${PATCH_BRANCH}" ]];then
      echo "WARNING: Current git branch is ${currentbranch} but patch is built for ${PATCH_BRANCH}."
      echo "WARNING: Continuing anyway..."
      PATCH_BRANCH=${currentbranch}
    fi
  fi

  determine_issue

  GIT_REVISION=$(${GIT} rev-parse --verify --short HEAD)
  # shellcheck disable=SC2034
  VERSION=${GIT_REVISION}_${ISSUE}_PATCH-${patchNum}

  if [[ "${ISSUE}" == 'Unknown' ]]; then
    echo "Testing patch on ${PATCH_BRANCH}."
  else
    echo "Testing ${ISSUE} patch on ${PATCH_BRANCH}."
  fi

  add_footer_table "git revision" "${PATCH_BRANCH} / ${GIT_REVISION}"

  return 0
}

## @description  Confirm the given branch is a git reference
## @descriptoin  or a valid gitXYZ commit hash
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        branch
## @return       0 on success, if gitXYZ was passed, PATCH_BRANCH=xyz
## @return       1 on failure
function verify_valid_branch
{
  local check=$1
  local i
  local hash

  # shortcut some common
  # non-resolvable names
  if [[ -z ${check} ]]; then
    return 1
  fi

  if [[ ${check} =~ ^git ]]; then
    hash=$(echo "${check}" | cut -f2- -dt)
    if [[ -n ${hash} ]]; then
      ${GIT} cat-file -t "${hash}" >/dev/null 2>&1
      if [[ $? -eq 0 ]]; then
        PATCH_BRANCH=${hash}
        return 0
      fi
      return 1
    else
      return 1
    fi
  fi

  ${GIT} show-ref "${check}" >/dev/null 2>&1
  return $?
}

## @description  Try to guess the branch being tested using a variety of heuristics
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success, with PATCH_BRANCH updated appropriately
## @return       1 on failure, with PATCH_BRANCH updated to PATCH_BRANCH_DEFAULT
function determine_branch
{
  local patchnamechunk
  local total
  local count

  # something has already set this, so move on
  if [[ -n ${PATCH_BRANCH} ]]; then
    return
  fi

  pushd "${BASEDIR}" > /dev/null

  yetus_debug "Determine branch"

  # something has already set this, so move on
  if [[ -n ${PATCH_BRANCH} ]]; then
    return
  fi

  # developer mode, existing checkout, whatever
  if [[ "${DIRTY_WORKSPACE}" == true ]];then
    PATCH_BRANCH=$(${GIT} rev-parse --abbrev-ref HEAD)
    echo "dirty workspace mode; applying against existing branch"
    return
  fi

  for j in "${PATCHURL}" "${PATCH_OR_ISSUE}"; do
    if [[ -z "${j}" ]]; then
      continue
    fi
    yetus_debug "Determine branch: starting with ${j}"
    patchnamechunk=$(echo "${j}" \
            | ${SED} -e 's,.*/\(.*\)$,\1,' \
                     -e 's,\.txt,.,' \
                     -e 's,.patch,.,g' \
                     -e 's,.diff,.,g' \
                     -e 's,\.\.,.,g' \
                     -e 's,\.$,,g' )

    # ISSUE-branch-##
    PATCH_BRANCH=$(echo "${patchnamechunk}" | cut -f3- -d- | cut -f1,2 -d-)
    yetus_debug "Determine branch: ISSUE-branch-## = ${PATCH_BRANCH}"
    if [[ -n "${PATCH_BRANCH}" ]]; then
      verify_valid_branch  "${PATCH_BRANCH}"
      if [[ $? == 0 ]]; then
        return
      fi
    fi

    # ISSUE-##[.##].branch
    PATCH_BRANCH=$(echo "${patchnamechunk}" | cut -f3- -d. )
    count="${PATCH_BRANCH//[^.]}"
    total=${#count}
    ((total = total + 3 ))
    until [[ ${total} -eq 2 ]]; do
      PATCH_BRANCH=$(echo "${patchnamechunk}" | cut -f3-${total} -d.)
      yetus_debug "Determine branch: ISSUE[.##].branch = ${PATCH_BRANCH}"
      ((total=total-1))
      if [[ -n "${PATCH_BRANCH}" ]]; then
        verify_valid_branch  "${PATCH_BRANCH}"
        if [[ $? == 0 ]]; then
          return
        fi
      fi
    done

    # ISSUE.branch.##
    PATCH_BRANCH=$(echo "${patchnamechunk}" | cut -f2- -d. )
    count="${PATCH_BRANCH//[^.]}"
    total=${#count}
    ((total = total + 3 ))
    until [[ ${total} -eq 2 ]]; do
      PATCH_BRANCH=$(echo "${patchnamechunk}" | cut -f2-${total} -d.)
      yetus_debug "Determine branch: ISSUE.branch[.##] = ${PATCH_BRANCH}"
      ((total=total-1))
      if [[ -n "${PATCH_BRANCH}" ]]; then
        verify_valid_branch  "${PATCH_BRANCH}"
        if [[ $? == 0 ]]; then
          return
        fi
      fi
    done

    # ISSUE-branch.##
    PATCH_BRANCH=$(echo "${patchnamechunk}" | cut -f3- -d- | cut -f1- -d. )
    count="${PATCH_BRANCH//[^.]}"
    total=${#count}
    ((total = total + 1 ))
    until [[ ${total} -eq 1 ]]; do
      PATCH_BRANCH=$(echo "${patchnamechunk}" | cut -f3- -d- | cut -f1-${total} -d. )
      yetus_debug "Determine branch: ISSUE-branch[.##] = ${PATCH_BRANCH}"
      ((total=total-1))
      if [[ -n "${PATCH_BRANCH}" ]]; then
        verify_valid_branch  "${PATCH_BRANCH}"
        if [[ $? == 0 ]]; then
          return
        fi
      fi
    done

  done

  PATCH_BRANCH="${PATCH_BRANCH_DEFAULT}"
  popd >/dev/null
}

## @description  Try to guess the issue being tested using a variety of heuristics
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success, with ISSUE updated appropriately
## @return       1 on failure, with ISSUE updated to "Unknown"
function determine_issue
{
  local patchnamechunk
  local maybeissue

  yetus_debug "Determine issue"

  # we can shortcut jenkins
  if [[ ${JENKINS} == true ]]; then
    ISSUE=${PATCH_OR_ISSUE}
    return 0
  fi

  # shellcheck disable=SC2016
  patchnamechunk=$(echo "${PATCH_OR_ISSUE}" | ${AWK} -F/ '{print $NF}')

  maybeissue=$(echo "${patchnamechunk}" | cut -f1,2 -d-)

  if [[ ${maybeissue} =~ ${ISSUE_RE} ]]; then
    ISSUE=${maybeissue}
    return 0
  fi

  ISSUE="Unknown"
  return 1
}

## @description  Add the given test type
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        test
function add_test
{
  local testname=$1

  yetus_debug "Testing against ${testname}"

  if [[ -z ${NEEDED_TESTS} ]]; then
    yetus_debug "Setting tests to ${testname}"
    NEEDED_TESTS=${testname}
  elif [[ ! ${NEEDED_TESTS} =~ ${testname} ]] ; then
    yetus_debug "Adding ${testname}"
    NEEDED_TESTS="${NEEDED_TESTS} ${testname}"
  fi
}

## @description  Verify if a given test was requested
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        test
## @return       1 = yes
## @return       0 = no
function verify_needed_test
{
  local i=$1

  if [[ ${NEEDED_TESTS} =~ $i ]]; then
    return 1
  fi
  return 0
}

## @description  Use some heuristics to determine which long running
## @description  tests to run
## @audience     private
## @stability    stable
## @replaceable  no
function determine_needed_tests
{
  local i

  for i in ${CHANGED_FILES}; do
    yetus_debug "Determining needed tests for ${i}"
    personality_file_tests "${i}"

    for plugin in ${PLUGINS}; do
      if declare -f ${plugin}_filefilter >/dev/null 2>&1; then
        "${plugin}_filefilter" "${i}"
      fi
    done
  done

  add_footer_table "Optional Tests" "${NEEDED_TESTS}"
}

## @description  Given ${PATCH_ISSUE}, determine what type of patch file is in use, and do the
## @description  necessary work to place it into ${PATCH_DIR}/patch.
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure, may exit
function locate_patch
{
  local notSureIfPatch=false
  yetus_debug "locate patch"

  if [[ -f ${PATCH_OR_ISSUE} ]]; then
    PATCH_FILE="${PATCH_OR_ISSUE}"
  else
    if [[ ${PATCH_OR_ISSUE} =~ ^http ]]; then
      echo "Patch is being downloaded at $(date) from"
      PATCHURL="${PATCH_OR_ISSUE}"
    else
      ${WGET} -q -O "${PATCH_DIR}/jira" "http://issues.apache.org/jira/browse/${PATCH_OR_ISSUE}"

      case $? in
        0)
        ;;
        2)
          yetus_error "ERROR: .wgetrc/.netrc parsing error."
          cleanup_and_exit 1
        ;;
        3)
          yetus_error "ERROR: File IO error."
          cleanup_and_exit 1
        ;;
        4)
          yetus_error "ERROR: URL ${PATCH_OR_ISSUE} is unreachable."
          cleanup_and_exit 1
        ;;
        *)
          # we want to try and do as much as we can in docker mode,
          # but if the patch was passed as a file, then we may not
          # be able to continue.
          if [[ ${REEXECED} == true
              && -f "${PATCH_DIR}/patch" ]]; then
            PATCH_FILE="${PATCH_DIR}/patch"
          else
            yetus_error "ERROR: Unable to fetch ${PATCH_OR_ISSUE}."
            cleanup_and_exit 1
          fi
          ;;
      esac

      if [[ -z "${PATCH_FILE}" ]]; then
        if [[ $(${GREP} -c 'Patch Available' "${PATCH_DIR}/jira") == 0 ]] ; then
          if [[ ${JENKINS} == true ]]; then
            yetus_error "ERROR: ${PATCH_OR_ISSUE} is not \"Patch Available\"."
            cleanup_and_exit 1
          else
            yetus_error "WARNING: ${PATCH_OR_ISSUE} is not \"Patch Available\"."
          fi
        fi

        #shellcheck disable=SC2016
        relativePatchURL=$(${AWK} 'match($0,"\"/jira/secure/attachment/[0-9]*/[^\"]*"){print substr($0,RSTART+1,RLENGTH-1)}' "${PATCH_DIR}/jira" |
          ${GREP} -v -e 'htm[l]*$' | sort | tail -1)
        PATCHURL="http://issues.apache.org${relativePatchURL}"
        if [[ ! ${PATCHURL} =~ \.patch$ ]]; then
          notSureIfPatch=true
        fi
        #shellcheck disable=SC2016
        patchNum=$(echo "${PATCHURL}" | ${AWK} 'match($0,"[0-9]*/"){print substr($0,RSTART,RLENGTH-1)}')
        echo "${ISSUE} patch is being downloaded at $(date) from"
      fi
    fi
    if [[ -z "${PATCH_FILE}" ]]; then
      echo "${PATCHURL}"
      add_footer_table "Patch URL" "${PATCHURL}"
      ${WGET} -q -O "${PATCH_DIR}/patch" "${PATCHURL}"
      if [[ $? != 0 ]];then
        yetus_error "ERROR: ${PATCH_OR_ISSUE} could not be downloaded."
        cleanup_and_exit 1
      fi
      PATCH_FILE="${PATCH_DIR}/patch"
    fi
  fi

  if [[ ! -f "${PATCH_DIR}/patch" ]]; then
    cp "${PATCH_FILE}" "${PATCH_DIR}/patch"
    if [[ $? == 0 ]] ; then
      echo "Patch file ${PATCH_FILE} copied to ${PATCH_DIR}"
    else
      yetus_error "ERROR: Could not copy ${PATCH_FILE} to ${PATCH_DIR}"
      cleanup_and_exit 1
    fi
  fi

  if [[ ${notSureIfPatch} == "true" ]]; then
    guess_patch_file "${PATCH_DIR}/patch"
    if [[ $? != 0 ]]; then
      yetus_error "ERROR: ${PATCHURL} is not a patch file."
      cleanup_and_exit 1
    else
      yetus_debug "The patch ${PATCHURL} was not named properly, but it looks like a patch file. proceeding, but issue/branch matching might go awry."
      add_vote_table 0 patch "The patch file was not named according to ${PROJECT_NAME}'s naming conventions. Please see ${HOW_TO_CONTRIBUTE} for instructions."
    fi
  fi
}

## @description Given a possible patch file, guess if it's a patch file without using smart-apply-patch
## @audience private
## @stability evolving
## @param path to patch file to test
## @return 0 we think it's a patch file
## @return 1 we think it's not a patch file
function guess_patch_file
{
  local patch=$1
  local fileOutput

  yetus_debug "Trying to guess is ${patch} is a patch file."
  fileOutput=$("${FILE}" "${patch}")
  if [[ $fileOutput =~ \ diff\  ]]; then
    yetus_debug "file magic says it's a diff."
    return 0
  fi
  fileOutput=$(head -n 1 "${patch}" | "${GREP}" -E "^(From [a-z0-9]* Mon Sep 17 00:00:00 2001)|(diff .*)|(Index: .*)$")
  if [[ $? == 0 ]]; then
    yetus_debug "first line looks like a patch file."
    return 0
  fi
  return 1
}

## @description  Given ${PATCH_DIR}/patch, verify the patch is good using ${BINDIR}/smart-apply-patch.sh
## @description  in dryrun mode.
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function verify_patch_file
{
  # Before building, check to make sure that the patch is valid
  export PATCH

  "${BINDIR}/smart-apply-patch.sh" --dry-run "${PATCH_DIR}/patch"
  if [[ $? != 0 ]] ; then
    echo "PATCH APPLICATION FAILED"
    add_vote_table -1 patch "The patch command could not apply the patch during dryrun."
    return 1
  else
    return 0
  fi
}

## @description  Given ${PATCH_DIR}/patch, apply the patch using ${BINDIR}/smart-apply-patch.sh
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       exit on failure
function apply_patch_file
{
  big_console_header "Applying patch"

  export PATCH
  "${BINDIR}/smart-apply-patch.sh" "${PATCH_DIR}/patch"
  if [[ $? != 0 ]] ; then
    echo "PATCH APPLICATION FAILED"
    ((RESULT = RESULT + 1))
    add_vote_table -1 patch "The patch command could not apply the patch."
    output_to_console 1
    output_to_bugsystem 1
    cleanup_and_exit 1
  fi
  return 0
}

## @description  copy the test-patch binary bits to a new working dir,
## @description  setting USER_PLUGIN_DIR and PERSONALITY to the new
## @description  locations.
## @description  this is used for test-patch in docker and reexec mode
## @audience     private
## @stability    evolving
## @replaceable  no
function copytpbits
{
  local dockerdir
  local dockfile
  local person
  # we need to copy/consolidate all the bits that might have changed
  # that are considered part of test-patch.  This *might* break
  # things that do off-path includes, but there isn't much we can
  # do about that, I don't think.

  # if we've already copied, then don't bother doing it again
  if [[ ${STARTDIR} == ${PATCH_DIR}/precommit ]]; then
    yetus_debug "Skipping copytpbits; already copied once"
    return
  fi

  pushd "${STARTINGDIR}" >/dev/null
  mkdir -p "${PATCH_DIR}/precommit/user-plugins"
  mkdir -p "${PATCH_DIR}/precommit/personality"
  mkdir -p "${PATCH_DIR}/precommit/test-patch-docker"

  # copy our entire universe, preserving links, etc.
  (cd "${BINDIR}"; tar cpf - . ) | (cd "${PATCH_DIR}/precommit"; tar xpf - )

  if [[ -n "${USER_PLUGIN_DIR}"
    && -d "${USER_PLUGIN_DIR}"  ]]; then
    cp -pr "${USER_PLUGIN_DIR}/*" \
      "${PATCH_DIR}/precommit/user-plugins"
  fi
  # Set to be relative to ${PATCH_DIR}/precommit
  USER_PLUGIN_DIR="${PATCH_DIR}/precommit/user-plugins"

  if [[ -n ${PERSONALITY}
    && -f ${PERSONALITY} ]]; then
    cp -pr "${PERSONALITY}" "${PATCH_DIR}/precommit/personality"
    person=$(basename "${PERSONALITY}")

    # Set to be relative to ${PATCH_DIR}/precommit
    PERSONALITY="${PATCH_DIR}/precommit/personality/${person}"
  fi

  if [[ -n ${DOCKERFILE}
      && -f ${DOCKERFILE} ]]; then
    dockerdir=$(dirname "${DOCKERFILE}")
    dockfile=$(basename "${DOCKERFILE}")
    pushd "${dockerdir}" >/dev/null
    gitfilerev=$("${GIT}" log -n 1 --pretty=format:%h -- "${dockfile}" 2>/dev/null)
    popd >/dev/null
    if [[ -z ${gitfilerev} ]]; then
      gitfilerev=$(date "+%F")
      gitfilerev="date${gitfilerev}"
    fi
    (
      echo "### TEST_PATCH_PRIVATE: dockerfile=${DOCKERFILE}"
      echo "### TEST_PATCH_PRIVATE: gitrev=${gitfilerev}"
      cat "${DOCKERFILE}"
      # make sure we put some space between, just in case last
      # line isn't an empty line or whatever
      printf "\n\n"
      cat "${BINDIR}/test-patch-docker/Dockerfile-endstub"

      printf "\n\n"
    ) > "${PATCH_DIR}/precommit/test-patch-docker/Dockerfile"
    DOCKERFILE="${PATCH_DIR}/precommit/test-patch-docker/Dockerfile"
  fi

  popd >/dev/null
}

## @description  If this patches actually patches test-patch.sh, then
## @description  run with the patched version for the test.
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       none; otherwise relaunches
function check_reexec
{
  local commentfile=${PATCH_DIR}/tp.${RANDOM}
  local tpdir
  local copy=false
  local testdir
  local person

  if [[ ${REEXECED} == true ]]; then
    big_console_header "Re-exec mode detected. Continuing."
    return
  fi

  for testdir in "${BINDIR}" \
      "${PERSONALITY}" \
      "${USER_PLUGIN_DIR}" \
      "${DOCKERFILE}"; do
    tpdir=$(relative_dir "${testdir}")
    if [[ $? == 0
        && ${CHANGED_FILES} =~ ${tpdir} ]]; then
      copy=true
    fi
  done

  if [[ ${copy} == true ]]; then
    big_console_header "precommit patch detected"

    if [[ ${RESETREPO} == false ]]; then
      ((RESULT = RESULT + 1))
      yetus_debug "can't destructively change the working directory. run with '--resetrepo' please. :("
      add_vote_table -1 precommit "Couldn't test precommit changes because we aren't configured to destructively change the working directory."
    else

      apply_patch_file

      if [[ ${JENKINS} == true ]]; then
        rm "${commentfile}" 2>/dev/null
        echo "(!) A patch to the testing environment has been detected. " > "${commentfile}"
        echo "Re-executing against the patched versions to perform further tests. " >> "${commentfile}"
        echo "The console is at ${BUILD_URL}console in case of problems." >> "${commentfile}"
        write_comment "${commentfile}"
        rm "${commentfile}"
      fi
    fi
  fi

  if [[ ${DOCKERSUPPORT} == false
     && ${copy} == false ]]; then
    return
  fi

  if [[ ${DOCKERSUPPORT} == true
      && ${copy} == false ]]; then
    big_console_header "Re-execing under Docker"

  fi

  # copy our universe
  copytpbits

  if [[ ${DOCKERSUPPORT} == true ]]; then
    # if we are doing docker, then we re-exec, but underneath the
    # container

    client=$(docker version | grep 'Client version' | cut -f2 -d: | tr -d ' ')
    server=$(docker version | grep 'Server version' | cut -f2 -d: | tr -d ' ')

    dockerversion="Client=${client} Server=${server}"

    TESTPATCHMODE="${USER_PARAMS[*]}"
    if [[ -n "${BUILD_URL}" ]]; then
      TESTPATCHMODE="--build-url=${BUILD_URL} ${TESTPATCHMODE}"
    fi
    TESTPATCHMODE="--tpglobaltimer=${GLOBALTIMER} ${TESTPATCHMODE}"
    TESTPATCHMODE="--tpreexectimer=${TIMER} ${TESTPATCHMODE}"
    TESTPATCHMODE="--personality=\'${PERSONALITY}\' ${TESTPATCHMODE}"
    TESTPATCHMODE="--plugins=\'${USER_PLUGIN_DIR}\' ${TESTPATCHMODE}"
    TESTPATCHMODE=" ${TESTPATCHMODE}"
    export TESTPATCHMODE

    patchdir=$(relative_dir "${PATCH_DIR}")

    cd "${BASEDIR}"
    #shellcheck disable=SC2093
    exec bash "${PATCH_DIR}/precommit/test-patch-docker/test-patch-docker.sh" \
       --dockerversion="${dockerversion}" \
       --java-home="${JAVA_HOME}" \
       --patch-dir="${patchdir}" \
       --project="${PROJECT_NAME}"

  else

    # if we aren't doing docker, then just call ourselves
    # but from the new path with the new flags
    #shellcheck disable=SC2093
    cd "${PATCH_DIR}/precommit/"
    exec "${PATCH_DIR}/precommit/test-patch.sh" \
      "${USER_PARAMS[@]}" \
      --reexec \
      --basedir="${BASEDIR}" \
      --branch="${PATCH_BRANCH}" \
      --patch-dir="${PATCH_DIR}" \
      --tpglobaltimer="${GLOBALTIMER}" \
      --tpreexectimer="${TIMER}" \
      --personality="${PERSONALITY}" \
      --plugins="${USER_PLUGIN_DIR}"
  fi
}

## @description  Reset the test results
## @audience     public
## @stability    evolving
## @replaceable  no
function modules_reset
{
  MODULE_STATUS=()
  MODULE_STATUS_TIMER=()
  MODULE_STATUS_MSG=()
  MODULE_STATUS_LOG=()
}

## @description  Utility to print standard module errors
## @audience     public
## @stability    evolving
## @replaceable  no
## @param        repostatus
## @param        testtype
## @param        mvncmdline
function modules_messages
{
  local repostatus=$1
  local testtype=$2
  local summarymode=$3
  shift 2
  local modindex=0
  local repo
  local goodtime=0
  local failure=false
  local oldtimer
  local statusjdk
  local multijdkmode=false

  if [[ ${repostatus} == branch ]]; then
    repo=${PATCH_BRANCH}
  else
    repo="the patch"
  fi

  verify_multijdk_test "${testtype}"
  if [[ $? == 1 ]]; then
    multijdkmode=true
  fi

  oldtimer=${TIMER}

  if [[ ${summarymode} == true
    && ${ALLOWSUMMARIES} == true ]]; then

    until [[ ${modindex} -eq ${#MODULE[@]} ]]; do

      if [[ ${multijdkmode} == true ]]; then
        statusjdk=${MODULE_STATUS_JDK[${modindex}]}
      fi

      if [[ "${MODULE_STATUS[${modindex}]}" == '+1' ]]; then
        ((goodtime=goodtime + ${MODULE_STATUS_TIMER[${modindex}]}))
      else
        failure=true
        start_clock
        echo ""
        echo "${MODULE_STATUS_MSG[${modindex}]}"
        echo ""
        offset_clock "${MODULE_STATUS_TIMER[${modindex}]}"
        add_vote_table "${MODULE_STATUS[${modindex}]}" "${testtype}" "${MODULE_STATUS_MSG[${modindex}]}"
        if [[ ${MODULE_STATUS[${modindex}]} == -1
          && -n "${MODULE_STATUS_LOG[${modindex}]}" ]]; then
          add_footer_table "${testtype}" "@@BASE@@/${MODULE_STATUS_LOG[${modindex}]}"
        fi
      fi
      ((modindex=modindex+1))
    done

    if [[ ${failure} == false ]]; then
      start_clock
      offset_clock "${goodtime}"
      add_vote_table +1 "${testtype}" "${repo} passed${statusjdk}"
    fi
  else
    until [[ ${modindex} -eq ${#MODULE[@]} ]]; do
      start_clock
      echo ""
      echo "${MODULE_STATUS_MSG[${modindex}]}"
      echo ""
      offset_clock "${MODULE_STATUS_TIMER[${modindex}]}"
      add_vote_table "${MODULE_STATUS[${modindex}]}" "${testtype}" "${MODULE_STATUS_MSG[${modindex}]}"
      if [[ ${MODULE_STATUS[${modindex}]} == -1
        && -n "${MODULE_STATUS_LOG[${modindex}]}" ]]; then
        add_footer_table "${testtype}" "@@BASE@@/${MODULE_STATUS_LOG[${modindex}]}"
      fi
      ((modindex=modindex+1))
    done
  fi
  TIMER=${oldtimer}
}

## @description  Add a test result
## @audience     public
## @stability    evolving
## @replaceable  no
## @param        module
## @param        runtime
function module_status
{
  local index=$1
  local value=$2
  local log=$3
  shift 3

  local jdk

  jdk=$(report_jvm_version "${JAVA_HOME}")

  if [[ -n ${index}
    && ${index} =~ ^[0-9]+$ ]]; then
    MODULE_STATUS[${index}]="${value}"
    MODULE_STATUS_LOG[${index}]="${log}"
    MODULE_STATUS_JDK[${index}]=" with JDK v${jdk}"
    MODULE_STATUS_MSG[${index}]="${*}"
  else
    yetus_error "ASSERT: module_status given bad index: ${index}"
    local frame=0
    while caller $frame; do
      ((frame++));
    done
    echo "$*"
    exit 1
  fi
}

## @description  run the maven tests for the queued modules
## @audience     public
## @stability    evolving
## @replaceable  no
## @param        repostatus
## @param        testtype
## @param        mvncmdline
function modules_workers
{
  local repostatus=$1
  local testtype=$2
  shift 2
  local modindex=0
  local fn
  local savestart=${TIMER}
  local savestop
  local repo
  local modulesuffix
  local jdk=""
  local jdkindex=0
  local statusjdk

  if [[ ${repostatus} == branch ]]; then
    repo=${PATCH_BRANCH}
  else
    repo="the patch"
  fi

  modules_reset

  verify_multijdk_test "${testtype}"
  if [[ $? == 1 ]]; then
    jdk=$(report_jvm_version "${JAVA_HOME}")
    statusjdk=" with JDK v${jdk}"
    jdk="-jdk${jdk}"
    jdk=${jdk// /}
    yetus_debug "Starting MultiJDK mode${statusjdk} on ${testtype}"
  fi

  until [[ ${modindex} -eq ${#MODULE[@]} ]]; do
    start_clock

    fn=$(module_file_fragment "${MODULE[${modindex}]}")
    fn="${fn}${jdk}"
    modulesuffix=$(basename "${MODULE[${modindex}]}")
    pushd "${BASEDIR}/${MODULE[${modindex}]}" >/dev/null

    if [[ ${modulesuffix} == . ]]; then
      modulesuffix="root"
    fi

    if [[ $? != 0 ]]; then
      echo "${BASEDIR}/${MODULE[${modindex}]} no longer exists. Skipping."
      ((modindex=modindex+1))
      continue
    fi

    case ${BUILDTOOL} in
      maven)
        #shellcheck disable=SC2086
        echo_and_redirect "${PATCH_DIR}/${repostatus}-${testtype}-${fn}.txt" \
          ${MVN} "${MAVEN_ARGS[@]}" \
            "${@//@@@MODULEFN@@@/${fn}}" \
             ${MODULEEXTRAPARAM[${modindex}]//@@@MODULEFN@@@/${fn}} -Ptest-patch
      ;;
      ant)
        #shellcheck disable=SC2086
        echo_and_redirect "${PATCH_DIR}/${repostatus}-${testtype}-${fn}.txt" \
          "${ANT}" "${ANT_ARGS[@]}" \
          ${MODULEEXTRAPARAM[${modindex}]//@@@MODULEFN@@@/${fn}} \
          "${@//@@@MODULEFN@@@/${fn}}"
      ;;
      *)
        yetus_error "ERROR: Unsupported build tool."
        return 1
      ;;
    esac

    if [[ $? == 0 ]] ; then
      module_status \
        ${modindex} \
        +1 \
        "${repostatus}-${testtype}-${fn}.txt" \
        "${modulesuffix} in ${repo} passed${statusjdk}."
    else
      module_status \
        ${modindex} \
        -1 \
        "${repostatus}-${testtype}-${fn}.txt" \
        "${modulesuffix} in ${repo} failed${statusjdk}."
      ((result = result + 1))
    fi
    savestop=$(stop_clock)
    MODULE_STATUS_TIMER[${modindex}]=${savestop}
    # shellcheck disable=SC2086
    echo "Elapsed: $(clock_display ${savestop})"
    popd >/dev/null
    ((modindex=modindex+1))
  done

  TIMER=${savestart}

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Reset the queue for tests
## @audience     public
## @stability    evolving
## @replaceable  no
function clear_personality_queue
{
  yetus_debug "Personality: clear queue"
  MODCOUNT=0
  MODULE=()
}

## @description  Build the queue for tests
## @audience     public
## @stability    evolving
## @replaceable  no
## @param        module
## @param        profiles/flags/etc
function personality_enqueue_module
{
  yetus_debug "Personality: enqueue $*"
  local module=$1
  shift

  MODULE[${MODCOUNT}]=${module}
  MODULEEXTRAPARAM[${MODCOUNT}]=${*}
  ((MODCOUNT=MODCOUNT+1))
}

## @description  Confirm compilation pre-patch
## @audience     private
## @stability    stable
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function precheck_javac
{
  local result=0
  local -r savejavahome=${JAVA_HOME}
  local multijdkmode=false
  local jdkindex=0

  big_console_header "Pre-patch ${PATCH_BRANCH} javac compilation"

  verify_needed_test javac
  if [[ $? == 0 ]]; then
     echo "Patch does not appear to need javac tests."
     return 0
  fi

  verify_multijdk_test javac
  if [[ $? == 1 ]]; then
    multijdkmode=true
  fi

  for jdkindex in ${JDK_DIR_LIST}; do
    if [[ ${multijdkmode} == true ]]; then
      JAVA_HOME=${jdkindex}
    fi

    personality_modules branch javac
    case ${BUILDTOOL} in
      maven)
        modules_workers branch javac clean test-compile
      ;;
      ant)
        modules_workers branch javac
      ;;
      *)
        yetus_error "ERROR: Unsupported build tool."
        return 1
      ;;
    esac

    ((result=result + $?))
    modules_messages branch javac true

  done
  JAVA_HOME=${savejavahome}

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Confirm Javadoc pre-patch
## @audience     private
## @stability    stable
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function precheck_javadoc
{
  local result=0
  local -r savejavahome=${JAVA_HOME}
  local multijdkmode=false
  local jdkindex=0

  big_console_header "Pre-patch ${PATCH_BRANCH} Javadoc verification"

  verify_needed_test javadoc
  if [[ $? == 0 ]]; then
     echo "Patch does not appear to need javadoc tests."
     return 0
  fi

  verify_multijdk_test javadoc
  if [[ $? == 1 ]]; then
    multijdkmode=true
  fi

  for jdkindex in ${JDK_DIR_LIST}; do
    if [[ ${multijdkmode} == true ]]; then
      JAVA_HOME=${jdkindex}
    fi

    personality_modules branch javadoc
    case ${BUILDTOOL} in
      maven)
        modules_workers branch javadoc clean javadoc:javadoc
      ;;
      ant)
        modules_workers branch javadoc clean javadoc
      ;;
      *)
        yetus_error "ERROR: Unsupported build tool."
        return 1
      ;;
    esac

    ((result=result + $?))
    modules_messages branch javadoc true

  done
  JAVA_HOME=${savejavahome}

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Confirm site pre-patch
## @audience     private
## @stability    stable
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function precheck_site
{
  local result=0

  if [[ ${BUILDTOOL} != maven ]]; then
    return 0
  fi

  big_console_header "Pre-patch ${PATCH_BRANCH} site verification"

  verify_needed_test site
  if [[ $? == 0 ]];then
    echo "Patch does not appear to need site tests."
    return 0
  fi

  personality_modules branch site
  modules_workers branch site clean site site:stage
  result=$?
  modules_messages branch site true
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Confirm the source environment pre-patch
## @audience     private
## @stability    stable
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function precheck_without_patch
{
  local result=0

  precheck_mvninstall

  if [[ $? -gt 0 ]]; then
    ((result = result +1 ))
  fi

  precheck_javac

  if [[ $? -gt 0 ]]; then
    ((result = result +1 ))
  fi

  precheck_javadoc

  if [[ $? -gt 0 ]]; then
    ((result = result +1 ))
  fi

  precheck_site

  if [[ $? -gt 0 ]]; then
    ((result = result +1 ))
  fi

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi

  return 0
}

## @description  Check the current directory for @author tags
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function check_author
{
  local authorTags
  local -r appname=$(basename "${BASH_SOURCE-$0}")

  big_console_header "Checking there are no @author tags in the patch."

  start_clock

  if [[ ${CHANGED_FILES} =~ ${appname} ]]; then
    echo "Skipping @author checks as ${appname} has been patched."
    add_vote_table 0 @author "Skipping @author checks as ${appname} has been patched."
    return 0
  fi

  authorTags=$("${GREP}" -c -i '^[^-].*@author' "${PATCH_DIR}/patch")
  echo "There appear to be ${authorTags} @author tags in the patch."
  if [[ ${authorTags} != 0 ]] ; then
    add_vote_table -1 @author \
      "The patch appears to contain ${authorTags} @author tags which the" \
      " community has agreed to not allow in code contributions."
    return 1
  fi
  add_vote_table +1 @author "The patch does not contain any @author tags."
  return 0
}

## @description  Check the patch file for changed/new tests
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function check_modified_unittests
{
  local testReferences=0
  local i

  big_console_header "Checking there are new or changed tests in the patch."

  verify_needed_test unit

  if [[ $? == 0 ]]; then
    echo "Patch does not appear to need new or modified tests."
    return 0
  fi

  start_clock

  for i in ${CHANGED_FILES}; do
    if [[ ${i} =~ (^|/)test/ ]]; then
      ((testReferences=testReferences + 1))
    fi
  done

  echo "There appear to be ${testReferences} test file(s) referenced in the patch."
  if [[ ${testReferences} == 0 ]] ; then
    add_vote_table -1 "test4tests" \
      "The patch doesn't appear to include any new or modified tests. " \
      "Please justify why no new tests are needed for this patch." \
      "Also please list what manual steps were performed to verify this patch."
    return 1
  fi
  add_vote_table +1 "test4tests" \
    "The patch appears to include ${testReferences} new or modified test files."
  return 0
}

## @description  Helper for check_patch_javac
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function count_javac_probs
{
  local warningfile=$1
  local val1
  local val2

  case ${BUILDTOOL} in
    maven)
      #shellcheck disable=SC2016,SC2046
      ${GREP} '\[WARNING\]' "${warningfile}" | ${AWK} '{sum+=1} END {print sum}'
    ;;
    ant)
      #shellcheck disable=SC2016
      val1=$(${GREP} -E "\[javac\] [0-9]+ errors?$" "${warningfile}" | ${AWK} '{sum+=$2} END {print sum}')
      #shellcheck disable=SC2016
      val2=$(${GREP} -E "\[javac\] [0-9]+ warnings?$" "${warningfile}" | ${AWK} '{sum+=$2} END {print sum}')
      echo $((val1+val2))
    ;;
  esac
}

## @description  Count and compare the number of javac warnings pre- and post- patch
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function check_patch_javac
{
  local i
  local result=0
  local fn
  local -r savejavahome=${JAVA_HOME}
  local multijdkmode=false
  local jdk=""
  local jdkindex=0
  local statusjdk
  declare -i numbranch=0
  declare -i numpatch=0

  big_console_header "Determining number of patched javac errors"

  verify_needed_test javac

  if [[ $? == 0 ]]; then
    echo "Patch does not appear to need javac tests."
    return 0
  fi

  verify_multijdk_test javac
  if [[ $? == 1 ]]; then
    multijdkmode=true
  fi

  for jdkindex in ${JDK_DIR_LIST}; do
    if [[ ${multijdkmode} == true ]]; then
      JAVA_HOME=${jdkindex}
      jdk=$(report_jvm_version "${JAVA_HOME}")
      yetus_debug "Using ${JAVA_HOME} to run this set of tests"
      statusjdk=" with JDK v${jdk}"
      jdk="-jdk${jdk}"
      jdk=${jdk// /}
    fi

    personality_modules patch javac

    case ${BUILDTOOL} in
      maven)
        modules_workers patch javac clean test-compile
      ;;
      ant)
        modules_workers patch javac
      ;;
      *)
        yetus_error "ERROR: Unsupported build tool."
        return 1
      ;;
    esac

    i=0
    until [[ ${i} -eq ${#MODULE[@]} ]]; do
      if [[ ${MODULE_STATUS[${i}]} == -1 ]]; then
        ((result=result+1))
        ((i=i+1))
        continue
      fi

      fn=$(module_file_fragment "${MODULE[${i}]}")
      fn="${fn}${jdk}"
      module_suffix=$(basename "${MODULE[${i}]}")
      if [[ ${module_suffix} == \. ]]; then
        module_suffix=root
      fi

      # if it was a new module, this won't exist.
      if [[ -f "${PATCH_DIR}/branch-javac-${fn}.txt" ]]; then
        ${GREP} -i warning "${PATCH_DIR}/branch-javac-${fn}.txt" \
          > "${PATCH_DIR}/branch-javac-${fn}-warning.txt"
      else
        touch "${PATCH_DIR}/branch-javac-${fn}.txt" \
          "${PATCH_DIR}/branch-javac-${fn}-warning.txt"
      fi

      if [[ -f "${PATCH_DIR}/patch-javac-${fn}.txt" ]]; then
        ${GREP} -i warning "${PATCH_DIR}/patch-javac-${fn}.txt" \
          > "${PATCH_DIR}/patch-javac-${fn}-warning.txt"
      else
        touch "${PATCH_DIR}/patch-javac-${fn}.txt" \
          "${PATCH_DIR}/patch-javac-${fn}-warning.txt"
      fi

      numbranch=$(count_javac_probs "${PATCH_DIR}/branch-javac-${fn}-warning.txt")
      numpatch=$(count_javac_probs "${PATCH_DIR}/patch-javac-${fn}-warning.txt")

      if [[ -n ${numbranch}
          && -n ${numpatch}
          && ${numpatch} -gt ${numbranch} ]]; then

        ${DIFF} -u "${PATCH_DIR}/branch-javac-${fn}-warning.txt" \
          "${PATCH_DIR}/patch-javac-${fn}-warning.txt" \
          > "${PATCH_DIR}/javac-${fn}-diff.txt"

        module_status ${i} -1 "javac-${fn}-diff.txt" \
          "Patched ${module_suffix} generated "\
          "$((numpatch-numbranch)) additional warning messages${statusjdk}." \

        ((result=result+1))
      fi
      ((i=i+1))
    done

    modules_messages patch javac true
  done
  JAVA_HOME=${savejavahome}

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Helper for check_patch_javadoc
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function count_javadoc_probs
{
  local warningfile=$1
  local val1
  local val2

  case ${BUILDTOOL} in
    maven)
      #shellcheck disable=SC2016,SC2046
      ${GREP} -E "^[0-9]+ warnings?$" "${warningfile}" | ${AWK} '{sum+=$1} END {print sum}'
    ;;
    ant)
      #shellcheck disable=SC2016
      val1=$(${GREP} -E "\[javadoc\] [0-9]+ errors?$" "${warningfile}" | ${AWK} '{sum+=$2} END {print sum}')
      #shellcheck disable=SC2016
      val2=$(${GREP} -E "\[javadoc\] [0-9]+ warnings?$" "${warningfile}" | ${AWK} '{sum+=$2} END {print sum}')
      echo $((val1+val2))
    ;;
  esac
}

## @description  Count and compare the number of JavaDoc warnings pre- and post- patch
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function check_patch_javadoc
{
  local i
  local result=0
  local fn
  local -r savejavahome=${JAVA_HOME}
  local multijdkmode=false
  local jdk=""
  local jdkindex=0
  local statusjdk
  declare -i numbranch=0
  declare -i numpatch=0

  big_console_header "Determining number of patched javadoc warnings"

  verify_needed_test javadoc
    if [[ $? == 0 ]]; then
    echo "Patch does not appear to need javadoc tests."
    return 0
  fi

  verify_multijdk_test javadoc
  if [[ $? == 1 ]]; then
    multijdkmode=true
  fi

  for jdkindex in ${JDK_DIR_LIST}; do
    if [[ ${multijdkmode} == true ]]; then
      JAVA_HOME=${jdkindex}
      jdk=$(report_jvm_version "${JAVA_HOME}")
      yetus_debug "Using ${JAVA_HOME} to run this set of tests"
      statusjdk=" with JDK v${jdk}"
      jdk="-jdk${jdk}"
      jdk=${jdk// /}
    fi

    personality_modules patch javadoc
    case ${BUILDTOOL} in
      maven)
        modules_workers patch javadoc clean javadoc:javadoc
      ;;
      ant)
        modules_workers patch javadoc clean javadoc
      ;;
      *)
        yetus_error "ERROR: Unsupported build tool."
        return 1
      ;;
    esac

    i=0
    until [[ ${i} -eq ${#MODULE[@]} ]]; do
      if [[ ${MODULE_STATUS[${i}]} == -1 ]]; then
        ((result=result+1))
        ((i=i+1))
        continue
      fi

      fn=$(module_file_fragment "${MODULE[${i}]}")
      fn="${fn}${jdk}"
      module_suffix=$(basename "${MODULE[${i}]}")
      if [[ ${module_suffix} == \. ]]; then
        module_suffix=root
      fi

      if [[ -f "${PATCH_DIR}/branch-javadoc-${fn}.txt" ]]; then
        ${GREP} -i warning "${PATCH_DIR}/branch-javadoc-${fn}.txt" \
          > "${PATCH_DIR}/branch-javadoc-${fn}-warning.txt"
      else
        touch "${PATCH_DIR}/branch-javadoc-${fn}.txt" \
          "${PATCH_DIR}/branch-javadoc-${fn}-warning.txt"
      fi

      if [[ -f "${PATCH_DIR}/patch-javadoc-${fn}.txt" ]]; then
        ${GREP} -i warning "${PATCH_DIR}/patch-javadoc-${fn}.txt" \
          > "${PATCH_DIR}/patch-javadoc-${fn}-warning.txt"
      else
        touch "${PATCH_DIR}/patch-javadoc-${fn}.txt" \
          "${PATCH_DIR}/patch-javadoc-${fn}-warning.txt"
      fi

      numbranch=$(count_javadoc_probs "${PATCH_DIR}/branch-javadoc-${fn}.txt")
      numpatch=$(count_javadoc_probs "${PATCH_DIR}/patch-javadoc-${fn}.txt")

      if [[ -n ${numbranch}
          && -n ${numpatch}
          && ${numpatch} -gt ${numbranch} ]] ; then

        ${DIFF} -u "${PATCH_DIR}/branch-javadoc-${fn}-warning.txt" \
          "${PATCH_DIR}/patch-javadoc-${fn}-warning.txt" \
          > "${PATCH_DIR}/javadoc-${fn}-diff.txt"

        module_status ${i} -1  "javadoc-${fn}-diff.txt" \
          "Patched ${module_suffix} generated "\
          "$((numpatch-numbranch)) additional warning messages${statusjdk}."

        ((result=result+1))
      fi
      ((i=i+1))
    done

    modules_messages patch javadoc true
  done
  JAVA_HOME=${savejavahome}

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Make sure site still compiles
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function check_site
{
  local result=0

  if [[ ${BUILDTOOL} != maven ]]; then
    return 0
  fi

  big_console_header "Determining number of patched site errors"

  verify_needed_test site
  if [[ $? == 0 ]]; then
    echo "Patch does not appear to need site tests."
    return 0
  fi

  personality_modules patch site
  modules_workers patch site clean site site:stage -Dmaven.javadoc.skip=true
  result=$?
  modules_messages patch site true
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Verify mvn install works
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function precheck_mvninstall
{
  local result=0

  if [[ ${BUILDTOOL} != maven ]]; then
    return 0
  fi

  big_console_header "Verifying mvn install works"

  verify_needed_test javadoc
  retval=$?

  verify_needed_test javac
  ((retval = retval + $? ))
  if [[ ${retval} == 0 ]]; then
    echo "This patch does not appear to need mvn install checks."
    return 0
  fi

  personality_modules branch mvninstall
  modules_workers branch mvninstall -fae clean install -Dmaven.javadoc.skip=true
  result=$?
  modules_messages branch mvninstall true
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Verify mvn install works
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function check_mvninstall
{
  local result=0

  if [[ ${BUILDTOOL} != maven ]]; then
    return 0
  fi

  big_console_header "Verifying mvn install still works"

  verify_needed_test javadoc
  retval=$?

  verify_needed_test javac
  ((retval = retval + $? ))
  if [[ ${retval} == 0 ]]; then
    echo "This patch does not appear to need mvn install checks."
    return 0
  fi

  personality_modules patch mvninstall
  modules_workers patch mvninstall clean install -Dmaven.javadoc.skip=true
  result=$?
  modules_messages patch mvninstall true
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Make sure Maven's eclipse generation works.
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function check_mvn_eclipse
{
  if [[ ${BUILDTOOL} != maven ]]; then
    return 0
  fi

  big_console_header "Verifying mvn eclipse:eclipse still works"

  verify_needed_test javac
  if [[ $? == 0 ]]; then
    echo "Patch does not touch any java files. Skipping mvn eclipse:eclipse"
    return 0
  fi

  personality_modules patch eclipse
  modules_workers patch eclipse eclipse:eclipse
  result=$?
  modules_messages patch eclipse true
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Utility to push many tests into the failure list
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        testdesc
## @param        testlist
function populate_test_table
{
  local reason=$1
  shift
  local first=""
  local i

  for i in "$@"; do
    if [[ -z "${first}" ]]; then
      add_test_table "${reason}" "${i}"
      first="${reason}"
    else
      add_test_table " " "${i}"
    fi
  done
}

## @description  Run and verify the output of the appropriate unit tests
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function check_unittests
{
  local i
  local testsys
  local test_logfile
  local result=0
  local -r savejavahome=${JAVA_HOME}
  local multijdkmode=false
  local jdk=""
  local jdkindex=0
  local statusjdk
  local formatresult=0
  local needlog
  local unitlogs

  big_console_header "Running unit tests"

  verify_needed_test unit

  if [[ $? == 0 ]]; then
    echo "Existing unit tests do not test patched files. Skipping."
    return 0
  fi

  verify_multijdk_test unit
  if [[ $? == 1 ]]; then
    multijdkmode=true
  fi

  for jdkindex in ${JDK_DIR_LIST}; do
    if [[ ${multijdkmode} == true ]]; then
      JAVA_HOME=${jdkindex}
      jdk=$(report_jvm_version "${JAVA_HOME}")
      statusjdk="JDK v${jdk} "
      jdk="-jdk${jdk}"
      jdk=${jdk// /}
    fi

    personality_modules patch unit
    case ${BUILDTOOL} in
      maven)
        modules_workers patch unit clean test -fae
      ;;
      ant)
        modules_workers patch unit
      ;;
      *)
        yetus_error "ERROR: Unsupported build tool."
        return 1
      ;;
    esac
    ((result=result+$?))

    modules_messages patch unit false
    if [[ ${result} == 0 ]]; then
      continue
    fi

    i=0
    until [[ $i -eq ${#MODULE[@]} ]]; do
      module=${MODULE[${i}]}
      fn=$(module_file_fragment "${module}")
      fn="${fn}${jdk}"
      test_logfile="${PATCH_DIR}/patch-unit-${fn}.txt"

      pushd "${MODULE[${i}]}" >/dev/null

      needlog=0
      for testsys in ${TESTFORMATS}; do
        if declare -f ${testsys}_process_tests >/dev/null; then
          yetus_debug "Calling ${testsys}_process_tests"
          "${testsys}_process_tests" "${module}" "${test_logfile}" "${fn}"
          formatresult=$?
          ((results=results+formatresult))
          if [[ "${formatresult}" != 0 ]]; then
            needlog=1
          fi
        fi
      done

      if [[ ${needlog} == 1 ]]; then
        unitlogs="${unitlogs} @@BASE@@/patch-unit-${fn}.txt"
      fi

      popd >/dev/null

      ((i=i+1))
    done

  done
  JAVA_HOME=${savejavahome}

  if [[ -n "${unitlogs}" ]]; then
    add_footer_table "unit test logs" "${unitlogs}"
  fi

  if [[ ${JENKINS} == true ]]; then
    add_footer_table "${statusjdk} Test Results" "${BUILD_URL}testReport/"
  fi

  for testsys in ${TESTFORMATS}; do
    if declare -f ${testsys}_finalize_results >/dev/null; then
      yetus_debug "Calling ${testsys}_finalize_results"
      "${testsys}_finalize_results" "${statusjdk}"
    fi
  done

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Print out the finished details on the console
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        runresult
## @return       0 on success
## @return       1 on failure
function output_to_console
{
  local result=$1
  shift
  local i=0
  local ourstring
  local vote
  local subs
  local ela
  local comment
  local commentfile1="${PATCH_DIR}/comment.1"
  local commentfile2="${PATCH_DIR}/comment.2"
  local normaltop
  local line
  local seccoladj=0
  local spcfx=${PATCH_DIR}/spcl.txt

  if [[ ${result} == 0 ]]; then
    if [[ ${JENKINS} == false ]]; then
      {
        printf "IF9fX19fX19fX18gCjwgU3VjY2VzcyEgPgogLS0tLS0tLS0tLSAKIFwgICAg";
        printf "IC9cICBfX18gIC9cCiAgXCAgIC8vIFwvICAgXC8gXFwKICAgICAoKCAgICBP";
        printf "IE8gICAgKSkKICAgICAgXFwgLyAgICAgXCAvLwogICAgICAgXC8gIHwgfCAg";
        printf "XC8gCiAgICAgICAgfCAgfCB8ICB8ICAKICAgICAgICB8ICB8IHwgIHwgIAog";
        printf "ICAgICAgIHwgICBvICAgfCAgCiAgICAgICAgfCB8ICAgfCB8ICAKICAgICAg";
        printf "ICB8bXwgICB8bXwgIAo"
      } > "${spcfx}"
    fi
    printf "\n\n+1 overall\n\n"
  else
    if [[ ${JENKINS} == false ]]; then
      {
        printf "IF9fX19fICAgICBfIF8gICAgICAgICAgICAgICAgXyAKfCAgX19ffF8gXyhf";
        printf "KSB8XyAgIF8gXyBfXyBfX198IHwKfCB8XyAvIF9gIHwgfCB8IHwgfCB8ICdf";
        printf "Xy8gXyBcIHwKfCAgX3wgKF98IHwgfCB8IHxffCB8IHwgfCAgX18vX3wKfF98";
        printf "ICBcX18sX3xffF98XF9fLF98X3wgIFxfX18oXykKICAgICAgICAgICAgICAg";
        printf "ICAgICAgICAgICAgICAgICAK"
      } > "${spcfx}"
    fi
    printf "\n\n-1 overall\n\n"
  fi

  if [[ -f ${spcfx} ]]; then
    if which base64 >/dev/null 2>&1; then
      base64 --decode "${spcfx}" 2>/dev/null
    elif which openssl >/dev/null 2>&1; then
      openssl enc -A -d -base64 -in "${spcfx}" 2>/dev/null
    fi
    echo
    echo
    rm "${spcfx}"
  fi

  seccoladj=$(findlargest 2 "${TP_VOTE_TABLE[@]}")
  if [[ ${seccoladj} -lt 10 ]]; then
    seccoladj=10
  fi

  seccoladj=$((seccoladj + 2 ))
  i=0
  until [[ $i -eq ${#TP_HEADER[@]} ]]; do
    printf "%s\n" "${TP_HEADER[${i}]}"
    ((i=i+1))
  done

  printf "| %s | %*s |  %s   | %s\n" "Vote" ${seccoladj} Subsystem Runtime "Comment"
  echo "============================================================================"
  i=0
  until [[ $i -eq ${#TP_VOTE_TABLE[@]} ]]; do
    ourstring=$(echo "${TP_VOTE_TABLE[${i}]}" | tr -s ' ')
    vote=$(echo "${ourstring}" | cut -f2 -d\|)
    subs=$(echo "${ourstring}"  | cut -f3 -d\|)
    ela=$(echo "${ourstring}" | cut -f4 -d\|)
    comment=$(echo "${ourstring}"  | cut -f5 -d\|)

    echo "${comment}" | fold -s -w $((78-seccoladj-22)) > "${commentfile1}"
    normaltop=$(head -1 "${commentfile1}")
    ${SED} -e '1d' "${commentfile1}"  > "${commentfile2}"

    printf "| %4s | %*s | %-10s |%-s\n" "${vote}" ${seccoladj} \
      "${subs}" "${ela}" "${normaltop}"
    while read line; do
      printf "|      | %*s |            | %-s\n" ${seccoladj} " " "${line}"
    done < "${commentfile2}"

    ((i=i+1))
    rm "${commentfile2}" "${commentfile1}" 2>/dev/null
  done

  if [[ ${#TP_TEST_TABLE[@]} -gt 0 ]]; then
    seccoladj=$(findlargest 1 "${TP_TEST_TABLE[@]}")
    printf "\n\n%*s | Tests\n" "${seccoladj}" "Reason"
    i=0
    until [[ $i -eq ${#TP_TEST_TABLE[@]} ]]; do
      ourstring=$(echo "${TP_TEST_TABLE[${i}]}" | tr -s ' ')
      vote=$(echo "${ourstring}" | cut -f2 -d\|)
      subs=$(echo "${ourstring}"  | cut -f3 -d\|)
      printf "%*s | %s\n" "${seccoladj}" "${vote}" "${subs}"
      ((i=i+1))
    done
  fi

  printf "\n\n|| Subsystem || Report/Notes ||\n"
  echo "============================================================================"
  i=0

  until [[ $i -eq ${#TP_FOOTER_TABLE[@]} ]]; do
    comment=$(echo "${TP_FOOTER_TABLE[${i}]}" |
              ${SED} -e "s,@@BASE@@,${PATCH_DIR},g")
    printf "%s\n" "${comment}"
    ((i=i+1))
  done
}

## @description  Write the final output to the selected bug system
## @audience     private
## @stability    evolving
## @replaceable  no
function output_to_bugsystem
{
  "${BUGSYSTEM}_finalreport" "${@}"
}

## @description  Clean the filesystem as appropriate and then exit
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        runresult
function cleanup_and_exit
{
  local result=$1

  if [[ ${JENKINS} == "true" && ${RELOCATE_PATCH_DIR} == "true" && \
      -e ${PATCH_DIR} && -d ${PATCH_DIR} ]] ; then
    # if PATCH_DIR is already inside BASEDIR, then
    # there is no need to move it since we assume that
    # Jenkins or whatever already knows where it is at
    # since it told us to put it there!
    relative_dir "${PATCH_DIR}" >/dev/null
    if [[ $? == 1 ]]; then
      yetus_debug "mv ${PATCH_DIR} ${BASEDIR}"
      mv "${PATCH_DIR}" "${BASEDIR}"
    fi
  fi
  big_console_header "Finished build."

  # shellcheck disable=SC2086
  exit ${result}
}

## @description  Driver to execute _postcheckout routines
## @audience     private
## @stability    evolving
## @replaceable  no
function postcheckout
{
  local routine
  local plugin

  for routine in find_java_home verify_patch_file; do
    verify_patchdir_still_exists

    yetus_debug "Running ${routine}"
    ${routine}

    (( RESULT = RESULT + $? ))
    if [[ ${RESULT} != 0 ]] ; then
      output_to_console 1
      output_to_bugsystem 1
      cleanup_and_exit 1
    fi
  done

  for plugin in ${PLUGINS}; do
    verify_patchdir_still_exists

    if declare -f ${plugin}_postcheckout >/dev/null 2>&1; then

      yetus_debug "Running ${plugin}_postcheckout"
      #shellcheck disable=SC2086
      ${plugin}_postcheckout

      (( RESULT = RESULT + $? ))
      if [[ ${RESULT} != 0 ]] ; then
        output_to_console 1
        output_to_bugsystem 1
        cleanup_and_exit 1
      fi
    fi
  done
}

## @description  Driver to execute _preapply routines
## @audience     private
## @stability    evolving
## @replaceable  no
function preapply
{
  local routine
  local plugin

  for routine in precheck_without_patch check_author \
                 check_modified_unittests
  do
    verify_patchdir_still_exists

    yetus_debug "Running ${routine}"
    ${routine}

    (( RESULT = RESULT + $? ))
  done

  for plugin in ${PLUGINS}; do
    verify_patchdir_still_exists

    if declare -f ${plugin}_preapply >/dev/null 2>&1; then

      yetus_debug "Running ${plugin}_preapply"
      #shellcheck disable=SC2086
      ${plugin}_preapply

      (( RESULT = RESULT + $? ))
    fi
  done
}

## @description  Driver to execute _postapply routines
## @audience     private
## @stability    evolving
## @replaceable  no
function postapply
{
  local routine
  local plugin
  local retval

  compute_gitdiff

  check_patch_javac
  retval=$?
  if [[ ${retval} -gt 1 ]] ; then
    output_to_console 1
    output_to_bugsystem 1
    cleanup_and_exit 1
  fi

  ((RESULT = RESULT + retval))

  # shellcheck disable=SC2043
  for routine in check_site
  do
    verify_patchdir_still_exists
    yetus_debug "Running ${routine}"
    ${routine}
    (( RESULT = RESULT + $? ))
  done

  for plugin in ${PLUGINS}; do
    verify_patchdir_still_exists
    if declare -f ${plugin}_postapply >/dev/null 2>&1; then
      yetus_debug "Running ${plugin}_postapply"
      #shellcheck disable=SC2086
      ${plugin}_postapply
      (( RESULT = RESULT + $? ))
    fi
  done
}

## @description  Driver to execute _postinstall routines
## @audience     private
## @stability    evolving
## @replaceable  no
function postinstall
{
  local routine
  local plugin

  verify_patchdir_still_exists
  for routine in check_patch_javadoc check_mvn_eclipse
  do
    verify_patchdir_still_exists
    yetus_debug "Running ${routine}"
    ${routine}
    (( RESULT = RESULT + $? ))
  done

  for plugin in ${PLUGINS}; do
    verify_patchdir_still_exists
    if declare -f ${plugin}_postinstall >/dev/null 2>&1; then
      yetus_debug "Running ${plugin}_postinstall"
      #shellcheck disable=SC2086
      ${plugin}_postinstall
      (( RESULT = RESULT + $? ))
    fi
  done
}

## @description  Driver to execute _tests routines
## @audience     private
## @stability    evolving
## @replaceable  no
function runtests
{
  local plugin

  ### Run tests for Jenkins or if explictly asked for by a developer
  if [[ ${JENKINS} == "true" || ${RUN_TESTS} == "true" ]] ; then

    verify_patchdir_still_exists
    check_unittests

    (( RESULT = RESULT + $? ))
  fi

  for plugin in ${PLUGINS}; do
    verify_patchdir_still_exists
    if declare -f ${plugin}_tests >/dev/null 2>&1; then
      yetus_debug "Running ${plugin}_tests"
      #shellcheck disable=SC2086
      ${plugin}_tests
      (( RESULT = RESULT + $? ))
    fi
  done
}

## @description  Import content from test-patch.d and optionally
## @description  from user provided plugin directory
## @audience     private
## @stability    evolving
## @replaceable  no
function importplugins
{
  local i
  local files=()

  if [[ ${LOAD_SYSTEM_PLUGINS} == "true" ]]; then
    if [[ -d "${BINDIR}/test-patch.d" ]]; then
      files=(${BINDIR}/test-patch.d/*.sh)
    fi
  fi

  if [[ -n "${USER_PLUGIN_DIR}" && -d "${USER_PLUGIN_DIR}" ]]; then
    yetus_debug "Loading user provided plugins from ${USER_PLUGIN_DIR}"
    files=("${files[@]}" ${USER_PLUGIN_DIR}/*.sh)
  fi

  for i in "${files[@]}"; do
    if [[ -f ${i} ]]; then
      yetus_debug "Importing ${i}"
      . "${i}"
    fi
  done

  if [[ -z ${PERSONALITY}
      && -f "${BINDIR}/personality/${PROJECT_NAME}.sh" ]]; then
    PERSONALITY="${BINDIR}/personality/${PROJECT_NAME}.sh"
  fi

  if [[ -n ${PERSONALITY} ]]; then
    if [[ ! -f ${PERSONALITY} ]]; then
      if [[ -f "${BINDIR}/personality/${PROJECT_NAME}.sh" ]]; then
        PERSONALITY="${BINDIR}/personality/${PROJECT_NAME}.sh"
      else
        yetus_debug "Can't find ${PERSONALITY} to import."
        return
      fi
    fi
    yetus_debug "Importing ${PERSONALITY}"
    . "${PERSONALITY}"
  fi
}

## @description  Let plugins also get a copy of the arguments
## @audience     private
## @stability    evolving
## @replaceable  no
function parse_args_plugins
{
  for plugin in ${PLUGINS} ${BUGSYSTEMS} ${TESTFORMATS}; do
    if declare -f ${plugin}_parse_args >/dev/null 2>&1; then
      yetus_debug "Running ${plugin}_parse_args"
      #shellcheck disable=SC2086
      ${plugin}_parse_args "$@"
      (( RESULT = RESULT + $? ))
    fi
  done
}

## @description  Register test-patch.d plugins
## @audience     public
## @stability    stable
## @replaceable  no
function add_plugin
{
  PLUGINS="${PLUGINS} $1"
}

## @description  Register test-patch.d bugsystems
## @audience     public
## @stability    stable
## @replaceable  no
function add_bugsystem
{
  BUGSYSTEMS="${BUGSYSTEMS} $1"
}

## @description  Register test-patch.d test output formats
## @audience     public
## @stability    stable
## @replaceable  no
function add_test_format
{
  TESTFORMATS="${TESTFORMATS} $1"
}

## @description  Calculate the differences between the specified files
## @description  and output it to stdout.
## @audience     public
## @stability    evolving
## @replaceable  no
function calcdiffs
{
  local orig=$1
  local new=$2
  local tmp=${PATCH_DIR}/pl.$$.${RANDOM}
  local count=0
  local j

  # first, pull out just the errors
  # shellcheck disable=SC2016
  ${AWK} -F: '{print $NF}' "${orig}" > "${tmp}.branch"

  # shellcheck disable=SC2016
  ${AWK} -F: '{print $NF}' "${new}" > "${tmp}.patch"

  # compare the errors, generating a string of line
  # numbers. Sorry portability: GNU diff makes this too easy
  ${DIFF} --unchanged-line-format="" \
     --old-line-format="" \
     --new-line-format="%dn " \
     "${tmp}.branch" \
     "${tmp}.patch" > "${tmp}.lined"

  # now, pull out those lines of the raw output
  # shellcheck disable=SC2013
  for j in $(cat "${tmp}.lined"); do
    # shellcheck disable=SC2086
    head -${j} "${new}" | tail -1
  done

  rm "${tmp}.branch" "${tmp}.patch" "${tmp}.lined" 2>/dev/null
}

###############################################################################
###############################################################################
###############################################################################

big_console_header "Bootstrapping test harness"

setup_defaults

parse_args "$@"

importplugins

parse_args_plugins "$@"

finish_docker_stats

locate_patch

# from here on out, we'll be in ${BASEDIR} for cwd
# routines need to pushd/popd if they change.
git_checkout
RESULT=$?
if [[ ${JENKINS} == "true" ]] ; then
  if [[ ${RESULT} != 0 ]] ; then
    exit 100
  fi
fi

find_changed_files

check_reexec

determine_needed_tests

postcheckout

fullyqualifyjdks

prepopulate_footer

find_changed_modules

preapply

apply_patch_file

# we find changed modules again
# in case the patch adds or removes a module
# this also means that test suites need to be
# aware that there might not be a 'before'
find_changed_modules

postapply

check_mvninstall

postinstall

runtests

finish_vote_table

finish_footer_table

output_to_console ${RESULT}
output_to_bugsystem ${RESULT}
cleanup_and_exit ${RESULT}
