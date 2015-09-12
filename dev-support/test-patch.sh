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
declare -a TP_HEADER
declare -a TP_VOTE_TABLE
declare -a TP_TEST_TABLE
declare -a TP_FOOTER_TABLE
declare -a MODULE_STATUS
declare -a MODULE_STATUS_TIMER
declare -a MODULE_STATUS_MSG
declare -a MODULE_STATUS_LOG
declare -a MODULE_COMPILE_LOG
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
  BUILD_NATIVE=${BUILD_NATIVE:-true}
  PATCH_BRANCH=""
  PATCH_BRANCH_DEFAULT="master"
  BUILDTOOLCWD=true

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
  TIMER=$(date +"%s")
  OSTYPE=$(uname -s)
  BUILDTOOL=maven
  TESTFORMATS=""
  JDK_TEST_LIST="compile javadoc unit"

  # Solaris needs POSIX, not SVID
  case ${OSTYPE} in
    SunOS)
      AWK=${AWK:-/usr/xpg4/bin/awk}
      SED=${SED:-/usr/xpg4/bin/sed}
      CURL=${CURL:-curl}
      GIT=${GIT:-git}
      GREP=${GREP:-/usr/xpg4/bin/grep}
      PATCH=${PATCH:-/usr/gnu/bin/patch}
      DIFF=${DIFF:-/usr/gnu/bin/diff}
      FILE=${FILE:-file}
    ;;
    *)
      AWK=${AWK:-awk}
      SED=${SED:-sed}
      CURL=${CURL:-curl}
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
  declare off=$1

  yetus_debug "offset clock by ${off}"

  if [[ -n ${off} ]]; then
    ((TIMER=TIMER-off))
  else
    yetus_error "ASSERT: no offset passed to offset_clock: ${index}"
    generate_stack
  fi
}

## @description generate a stack trace when in debug mode
## @audience     public
## @stability    stable
## @replaceable  no
## @return       exits
function generate_stack
{
  declare frame

  if [[ -n "${TP_SHELL_SCRIPT_DEBUG}" ]]; then
    while caller "${frame}"; do
      ((frame++));
    done
  fi
  exit 1
}

## @description  Add to the header of the display
## @audience     public
## @stability    stable
## @replaceable  no
## @param        string
function add_header_line
{
  # shellcheck disable=SC2034
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
    # shellcheck disable=SC2034
    TP_VOTE_TABLE[${TP_VOTE_COUNTER}]="|  | ${subsystem} | | ${*:-} |"
  else
    # shellcheck disable=SC2034
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

## @description  Put the opening environment information at the bottom
## @description  of the footer table
## @stability     stable
## @audience     private
## @replaceable  yes
function prepopulate_footer
{
  # shellcheck disable=SC2155
  declare -r unamea=$(uname -a)

  add_footer_table "uname" "${unamea}"
  add_footer_table "Build tool" "${BUILDTOOL}"

  if [[ -n ${PERSONALITY} ]]; then
    add_footer_table "Personality" "${PERSONALITY}"
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

  # shellcheck disable=SC2034
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

  # shellcheck disable=SC2034
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

  # shellcheck disable=SC2034
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

## @description Write the contents of a file to all of the bug systems
## @description (so content should avoid special formatting)
## @params filename
## @stability stable
## @audience public
function write_comment
{
  local -r commentfile=${1}
  declare bug

  for bug in ${BUGCOMMENTS}; do
    if declare -f ${bug}_write_comment >/dev/null; then
       "${bug}_write_comment" "${commentfile}"
    fi
  done
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
  while read -r line; do
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

  if [[ ! -f "${GITDIFFLINES}" ]]; then
    touch "${GITDIFFLINES}"
  fi

  if [[ ! -f "${GITDIFFCONTENT}" ]]; then
    touch "${GITDIFFCONTENT}"
  fi

  if [[ -s "${GITDIFFLINES}" ]]; then
    compute_unidiff
  else
    touch "${GITUNIDIFFLINES}"
  fi

  popd >/dev/null
}

## @description generate an index of unified diff lines vs. modified/added lines
## @description ${GITDIFFLINES} must exist.
## @audience    private
## @stability   stable
## @replaceable no
function compute_unidiff
{
  declare fn
  declare filen
  declare tmpfile="${PATCH_DIR}/tmp.$$.${RANDOM}"

  # now that we know what lines are where, we can deal
  # with github's pain-in-the-butt API. It requires
  # that the client provides the line number of the
  # unified diff on a per file basis.

  # First, build a per-file unified diff, pulling
  # out the 'extra' lines, grabbing the adds with
  # the line number in the diff file along the way,
  # finally rewriting the line so that it is in
  # './filename:diff line:content' format

  for fn in ${CHANGED_FILES}; do
    filen=${fn##./}

    if [[ -f "${filen}" ]]; then
      ${GIT} diff ${filen} \
        | tail -n +6 \
        | ${GREP} -n '^+' \
        | ${GREP} -vE '^[0-9]*:\+\+\+' \
        | ${SED} -e 's,^\([0-9]*:\)\+,\1,g' \
          -e s,^,./${filen}:,g \
              >>  "${tmpfile}"
    fi
  done

  # at this point, tmpfile should be in the same format
  # as gitdiffcontent, just with different line numbers.
  # let's do a merge (using gitdifflines because it's easier)

  # ./filename:real number:diff number
  # shellcheck disable=SC2016
  paste -d: "${GITDIFFLINES}" "${tmpfile}" \
    | ${AWK} -F: '{print $1":"$2":"$5":"$6}' \
    >> "${GITUNIDIFFLINES}"

  rm "${tmpfile}"
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
  echo "--build-native=<bool>  If true, then build native components (default 'true')"
  echo "--build-tool=<tool>    Pick which build tool to focus around (${BUILDTOOLS})"
  echo "--bugcomments=<bug>    Only write comments to the screen and this comma delimited list (${BUGSYSTEMS})"
  echo "--contrib-guide=<url>  URL to point new users towards project conventions. (default: ${HOW_TO_CONTRIBUTE} )"
  echo "--debug                If set, then output some extra stuff to stderr"
  echo "--dirty-workspace      Allow the local git workspace to have uncommitted changes"
  echo "--docker               Spawn a docker container"
  echo "--dockerfile=<file>    Dockerfile fragment to use as the base"
  echo "--java-home=<path>     Set JAVA_HOME (In Docker mode, this should be local to the image)"
  echo "--linecomments=<bug>   Only write line comments to this comma delimited list (defaults to bugcomments)"
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
  echo "--awk-cmd=<cmd>        The 'awk' command to use (default 'awk')"
  echo "--curl-cmd=<cmd>       The 'curl' command to use (default 'curl')"
  echo "--diff-cmd=<cmd>       The GNU-compatible 'diff' command to use (default 'diff')"
  echo "--file-cmd=<cmd>       The 'file' command to use (default 'file')"
  echo "--git-cmd=<cmd>        The 'git' command to use (default 'git')"
  echo "--grep-cmd=<cmd>       The 'grep' command to use (default 'grep')"
  echo "--patch-cmd=<cmd>      The 'patch' command to use (default 'patch')"
  echo "--sed-cmd=<cmd>        The 'sed' command to use (default 'sed')"

  echo
  echo "Jenkins-only options:"
  echo "--jenkins              Run by Jenkins (runs tests and posts results to JIRA)"
  echo "--build-url            Set the build location web page"
  echo "--mv-patch-dir         Move the patch-dir into the basedir during cleanup."

  importplugins

  for plugin in ${BUILDTOOLS} ${PLUGINS} ${BUGSYSTEMS} ${TESTFORMATS}; do
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
      --bugcomments=*)
        BUGCOMMENTS=${i#*=}
        BUGCOMMENTS=${BUGCOMMENTS//,/ }
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
      --curl-cmd=*)
        CURL=${i#*=}
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
      --java-home=*)
        JAVA_HOME=${i#*=}
      ;;
      --jenkins)
        JENKINS=true
        TEST_PARALLEL=${TEST_PARALLEL:-true}
      ;;
      --linecomments=*)
        BUGLINECOMMENTS=${i#*=}
        BUGLINECOMMENTS=${BUGLINECOMMENTS//,/ }
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
  GITUNIDIFFLINES="${PATCH_DIR}/gitdiffunilines.txt"

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

  buildfile=$("${BUILDTOOL}_buildfile")

  if [[ $? != 0 ]]; then
    yetus_error "ERROR: Unsupported build tool."
    bugsystem_finalreport 1
    cleanup_and_exit 1
  fi

  changed_dirs=$(for i in ${CHANGED_FILES}; do dirname "${i}"; done | sort -u)

  # Now find all the modules that were changed
  for i in ${changed_dirs}; do

    module_skipdir "${i}"
    if [[ $? != 0 ]]; then
      continue
    fi

    builddir=$(find_buildfile_dir "${buildfile}" "${i}")
    if [[ -z ${builddir} ]]; then
      yetus_error "ERROR: ${buildfile} is not found. Make sure the target is a ${BUILDTOOL}-based project."
      bugsystem_finalreport 1
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
  else
    buildmods=${CHANGED_UNFILTERED_MODULES}
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
  builddir=$(find_buildfile_dir "${buildfile}" "${builddir}" || true)

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
  local status

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

      if [[ -f .git/rebase-apply ]]; then
        yetus_error "ERROR: previous rebase failed. Aborting it."
        ${GIT} rebase --abort
      fi

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
  declare bugs
  declare retval=1

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

  for bugs in ${BUGSYSTEMS}; do
    if declare -f ${bugs}_determine_branch >/dev/null;then
      "${bugs}_determine_branch"
      retval=$?
      if [[ ${retval} == 0 ]]; then
        break
      fi
    fi
  done

  if [[ ${retval} != 0 ]]; then
    PATCH_BRANCH="${PATCH_BRANCH_DEFAULT}"
  fi
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
  local bugsys

  yetus_debug "Determine issue"

  for bugsys in ${BUGSYSTEMS}; do
    if declare -f ${bugsys}_determine_issue >/dev/null; then
      "${bugsys}_determine_issue" "${PATCH_OR_ISSUE}"
      if [[ $? == 0 ]]; then
        yetus_debug "${bugsys} says ${ISSUE}"
        return 0
      fi
    fi
  done
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
    NEEDED_TESTS="${NEEDED_TESTS} ${testname} "
  fi
}

## @description  Remove the given test type
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        test
function delete_test
{
  local testname=$1

  yetus_debug "Testing against ${testname}"

  if [[ ${NEEDED_TESTS} =~ ${testname} ]] ; then
    yetus_debug "Removing ${testname}"
    NEEDED_TESTS="${NEEDED_TESTS// ${testname} }"
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
  local plugin

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
  local bugsys
  local patchfile=""
  local gotit=false

  yetus_debug "locate patch"

  # it's a locally provided file
  if [[ -f ${PATCH_OR_ISSUE} ]]; then
    patchfile="${PATCH_OR_ISSUE}"
  else
    # run through the bug systems.  maybe they know?
    for bugsys in ${BUGSYSTEMS}; do
      if declare -f ${bugsys}_locate_patch >/dev/null 2>&1; then
        "${bugsys}_locate_patch" "${PATCH_OR_ISSUE}" "${PATCH_DIR}/patch"
        if [[ $? == 0 ]]; then
          guess_patch_file "${PATCH_DIR}/patch"
          if [[ $? == 0 ]]; then
            gotit=true
            break;
          fi
        fi
      fi
    done

    # ok, none of the bug systems know. let's see how smart we are
    if [[ ${gotit} == false ]]; then
      generic_locate_patch "${PATCH_OR_ISSUE}" "${PATCH_DIR}/patch"
    fi
  fi

  if [[ ! -f "${PATCH_DIR}/patch"
      && -f "${patchfile}" ]]; then
    cp "${patchfile}" "${PATCH_DIR}/patch"
    if [[ $? == 0 ]] ; then
      echo "Patch file ${patchfile} copied to ${PATCH_DIR}"
    else
      yetus_error "ERROR: Could not copy ${patchfile} to ${PATCH_DIR}"
      cleanup_and_exit 1
    fi
  fi

  guess_patch_file "${PATCH_DIR}/patch"
  if [[ $? != 0 ]]; then
    yetus_error "ERROR: Unsure how to process ${PATCH_OR_ISSUE}."
    cleanup_and_exit 1
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

  if [[ ! -f ${patch} ]]; then
    return 1
  fi

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
  big_console_header "Applying patch to ${PATCH_BRANCH}"

  export PATCH
  "${BINDIR}/smart-apply-patch.sh" "${PATCH_DIR}/patch"
  if [[ $? != 0 ]] ; then
    echo "PATCH APPLICATION FAILED"
    ((RESULT = RESULT + 1))
    add_vote_table -1 patch "The patch command could not apply the patch."
    bugsystem_finalreport 1
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
  MODULE_COMPILE_LOG=()
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
    yetus_error "ASSERT: module_stats $*"
    generate_stack
    exit 1
  fi
}

## @description  run the tests for the queued modules
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
  local result=0

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
    if [[ ${BUILDTOOLCWD} == true ]]; then
      pushd "${BASEDIR}/${MODULE[${modindex}]}" >/dev/null
    fi

    if [[ ${modulesuffix} == . ]]; then
      modulesuffix="root"
    fi

    if [[ $? != 0 ]]; then
      echo "${BASEDIR}/${MODULE[${modindex}]} no longer exists. Skipping."
      ((modindex=modindex+1))
      continue
    fi

    # shellcheck disable=2086,2046
    echo_and_redirect "${PATCH_DIR}/${repostatus}-${testtype}-${fn}.txt" \
      $("${BUILDTOOL}_executor") \
      ${MODULEEXTRAPARAM[${modindex}]//@@@MODULEFN@@@/${fn}} \
      "${@//@@@MODULEFN@@@/${fn}}"

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

    # compile is special
    if [[ ${testtype} = compile ]]; then
      MODULE_COMPILE_LOG[${modindex}]="${PATCH_DIR}/${repostatus}-${testtype}-${fn}.txt"
      yetus_debug "Comile log set to ${MODULE_COMPILE_LOG[${modindex}]}"
    fi

    savestop=$(stop_clock)
    MODULE_STATUS_TIMER[${modindex}]=${savestop}
    # shellcheck disable=SC2086
    echo "Elapsed: $(clock_display ${savestop})"
    if [[ ${BUILDTOOLCWD} == true ]]; then
      popd >/dev/null
    fi
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

  verify_needed_test unit

  if [[ $? == 0 ]]; then
    return 0
  fi

  big_console_header "Running unit tests"

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
    "${BUILDTOOL}_modules_worker" patch unit

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

      if [[ ${BUILDTOOLCWD} == true ]]; then
        pushd "${MODULE[${i}]}" >/dev/null
      fi

      needlog=0
      for testsys in ${TESTFORMATS}; do
        if declare -f ${testsys}_process_tests >/dev/null; then
          yetus_debug "Calling ${testsys}_process_tests"
          "${testsys}_process_tests" "${module}" "${test_logfile}" "${fn}"
          formatresult=$?
          ((result=result+formatresult))
          if [[ "${formatresult}" != 0 ]]; then
            needlog=1
          fi
        fi
      done

      if [[ ${needlog} == 1 ]]; then
        unitlogs="${unitlogs} @@BASE@@/patch-unit-${fn}.txt"
      fi

      if [[ ${BUILDTOOLCWD} == true ]]; then
        popd >/dev/null
      fi

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

## @description  Write comments onto bug systems that have code review support.
## @description  File should be in the form of "file:line:comment"
## @audience     public
## @stability    evolving
## @replaceable  no
## @param        filename
function bugsystem_linecomments
{
  declare title=$1
  declare fn=$2
  declare line
  declare bugs
  declare realline
  declare text
  declare idxline
  declare uniline

  if [[ ! -f "${GITUNIDIFFLINES}" ]]; then
    return
  fi

  while read -r line;do
    file=$(echo "${line}" | cut -f1 -d:)
    realline=$(echo "${line}" | cut -f2 -d:)
    text=$(echo "${line}" | cut -f3- -d:)
    idxline="${file}:${realline}:"
    uniline=$(${GREP} "${idxline}" "${GITUNIDIFFLINES}" | cut -f3 -d: )

    for bugs in ${BUGLINECOMMENTS}; do
      if declare -f ${bugs}_linecomments >/dev/null;then
        "${bugs}_linecomments" "${title}" "${file}" "${realline}" "${uniline}" "${text}"
      fi
    done
  done < "${fn}"
}

## @description  Write the final output to the selected bug system
## @audience     private
## @stability    evolving
## @replaceable  no
function bugsystem_finalreport
{
  declare bugs

  for bugs in ${BUGCOMMENTS}; do
    if declare -f ${bugs}_finalreport >/dev/null;then
      "${bugs}_finalreport" "${@}"
    fi
  done
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
      modules_reset
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
  for plugin in ${PLUGINS} ${BUGSYSTEMS} ${TESTFORMATS} ${BUILDTOOLS}; do
    if declare -f ${plugin}_parse_args >/dev/null 2>&1; then
      yetus_debug "Running ${plugin}_parse_args"
      #shellcheck disable=SC2086
      ${plugin}_parse_args "$@"
      (( RESULT = RESULT + $? ))
    fi
  done

  BUGCOMMENTS=${BUGCOMMENTS:-${BUGSYSTEMS}}
  if [[ ! ${BUGCOMMENTS} =~ console ]]; then
    BUGCOMMENTS="${BUGCOMMENTS} console"
  fi

  BUGLINECOMMENTS=${BUGLINECOMMENTS:-${BUGCOMMENTS}}
}

## @description  Let plugins also get a copy of the arguments
## @audience     private
## @stability    evolving
## @replaceable  no
function plugins_initialize
{
  declare plugin

  for plugin in ${PLUGINS} ${BUGSYSTEMS} ${TESTFORMATS} ${BUILDTOOLS}; do
    if declare -f ${plugin}_initialize >/dev/null 2>&1; then
      yetus_debug "Running ${plugin}_initialize"
      #shellcheck disable=SC2086
      ${plugin}_initialize
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

## @description  Register test-patch.d build tools
## @audience     public
## @stability    stable
## @replaceable  no
function add_build_tool
{
  BUILDTOOLS="${BUILDTOOLS} $1"
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

## @description  Helper routine for plugins to ask projects, etc
## @description  to count problems in a log file
## @description  and output it to stdout.
## @audience     public
## @stability    evolving
## @replaceable  no
## @return       number of issues
function generic_count_probs
{
  declare testtype=$1
  declare input=$2

  if declare -f ${PROJECT}_${testtype}_count_probs >/dev/null; then
    "${PROJECT}_${testtype}_count_probs" "${input}"
  elif declare -f ${BUILDTOOL}_${testtype}_count_probs >/dev/null; then
    "${BUILDTOOL}_${testtype}_count_probs" "${input}"
  else
    yetus_error "ERROR: ${testtype}: No function defined to count problems."
    echo 0
  fi
}

## @description  Helper routine for plugins to do a pre-patch prun
## @audience     public
## @stability    evolving
## @replaceable  no
## @param        testype
## @param        multijdk
## @return       1 on failure
## @return       0 on success
function generic_pre_handler
{
  declare testtype=$1
  declare multijdkmode=$2
  declare result=0
  declare -r savejavahome=${JAVA_HOME}
  declare multijdkmode=false
  declare jdkindex=0

  verify_needed_test "${testtype}"
  if [[ $? == 0 ]]; then
     return 0
  fi

  big_console_header "Pre-patch ${testtype} verification on ${PATCH_BRANCH}"

  verify_multijdk_test "${testtype}"
  if [[ $? == 1 ]]; then
    multijdkmode=true
  fi

  for jdkindex in ${JDK_DIR_LIST}; do
    if [[ ${multijdkmode} == true ]]; then
      JAVA_HOME=${jdkindex}
    fi

    personality_modules branch "${testtype}"
    "${BUILDTOOL}_modules_worker" branch "${testtype}"

    ((result=result + $?))
    modules_messages branch "${testtype}" true

  done
  JAVA_HOME=${savejavahome}

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Generic post-patch log handler
## @audience     public
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
## @param        origlog
## @param        testtype
## @param        multijdkmode
function generic_postlog_compare
{
  declare origlog=$1
  declare testtype=$2
  declare multijdk=$3
  declare result=0
  declare i
  declare fn
  declare jdk
  declare statusjdk

  if [[ ${multijdk} == true ]]; then
    jdk=$(report_jvm_version "${JAVA_HOME}")
    statusjdk=" with JDK v${jdk}"
    jdk="-jdk${jdk}"
    jdk=${jdk// /}
  fi

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

    yetus_debug "${testtype}: branch-${origlog}-${fn}.txt vs. patch-${origlog}-${fn}.txt"

    # if it was a new module, this won't exist.
    if [[ -f "${PATCH_DIR}/branch-${origlog}-${fn}.txt" ]]; then
      ${GREP} -i warning "${PATCH_DIR}/branch-${origlog}-${fn}.txt" \
        > "${PATCH_DIR}/branch-${testtype}-${fn}-warning.txt"
    else
      touch "${PATCH_DIR}/branch-${origlog}-${fn}.txt" \
        "${PATCH_DIR}/branch-${testtype}-${fn}-warning.txt"
    fi

    if [[ -f "${PATCH_DIR}/patch-${origlog}-${fn}.txt" ]]; then
      ${GREP} -i warning "${PATCH_DIR}/patch-${origlog}-${fn}.txt" \
        > "${PATCH_DIR}/patch-${testtype}-${fn}-warning.txt"
    else
      touch "${PATCH_DIR}/patch-${origlog}-${fn}.txt" \
        "${PATCH_DIR}/patch-${testtype}-${fn}-warning.txt"
    fi

    numbranch=$("generic_count_probs" "${testtype}" "${PATCH_DIR}/branch-${testtype}-${fn}-warning.txt")
    numpatch=$("generic_count_probs" "${testtype}" "${PATCH_DIR}/patch-${testtype}-${fn}-warning.txt")

    yetus_debug "${testtype}: old: ${numbranch} vs new: ${numpatch}"

    if [[ -n ${numbranch}
       && -n ${numpatch}
       && ${numpatch} -gt ${numbranch} ]]; then

      ${DIFF} -u "${PATCH_DIR}/branch-${testtype}-${fn}-warning.txt" \
        "${PATCH_DIR}/patch-${testtype}-${fn}-warning.txt" \
        > "${PATCH_DIR}/${testtype}-${fn}-diff.txt"

      add_vote_table -1 "${testtype}" "${fn}${statusjdk} has problems."
      add_footer_table "${testtype}" "${fn}: @@BASE@@/${testtype}-${fn}-diff.txt"

      ((result=result+1))
    fi
    ((i=i+1))
  done
  modules_messages patch "${testtype}" true
  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Generic post-patch handler
## @audience     public
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
## @param        origlog
## @param        testtype
## @param        multijdkmode
## @param        run commands
function generic_post_handler
{
  declare origlog=$1
  declare testtype=$2
  declare multijdkmode=$3
  declare need2run=$4
  declare i
  declare result=0
  declare fn
  declare -r savejavahome=${JAVA_HOME}
  declare jdk=""
  declare jdkindex=0
  declare statusjdk
  declare -i numbranch=0
  declare -i numpatch=0

  verify_needed_test "${testtype}"
  if [[ $? == 0 ]]; then
    yetus_debug "${testtype} not needed"
    return 0
  fi

  big_console_header "Patch ${testtype} verification"

  for jdkindex in ${JDK_DIR_LIST}; do
    if [[ ${multijdkmode} == true ]]; then
      JAVA_HOME=${jdkindex}
      yetus_debug "Using ${JAVA_HOME} to run this set of tests"
    fi

    if [[ ${need2run} = true ]]; then
      personality_modules "${codebase}" "${testtype}"
      "${BUILDTOOL}_modules_worker" "${codebase}" "${testtype}"

      if [[ ${UNSUPPORTED_TEST} = true ]]; then
        return 0
      fi
    fi

    generic_postlog_compare "${origlog}" "${testtype}" "${multijdkmode}"
    ((result=result+$?))
  done
  JAVA_HOME=${savejavahome}

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Execute the compile phase. This will callout
## @description  to _compile
## @audience     public
## @stability    evolving
## @replaceable  no
## @param        branch|patch
## @return       0 on success
## @return       1 on failure
function compile
{
  declare codebase=$1
  declare result=0
  declare -r savejavahome=${JAVA_HOME}
  declare multijdkmode=false
  declare jdkindex=0

  verify_needed_test compile
  if [[ $? == 0 ]]; then
     return 0
  fi

  if [[ ${codebase} = "branch" ]]; then
    big_console_header "Pre-patch ${PATCH_BRANCH} compilation"
  else
    big_console_header "Patch compilation"
  fi

  verify_multijdk_test compile
  if [[ $? == 1 ]]; then
    multijdkmode=true
  fi

  for jdkindex in ${JDK_DIR_LIST}; do
    if [[ ${multijdkmode} == true ]]; then
      JAVA_HOME=${jdkindex}
    fi

    personality_modules "${codebase}" compile
    "${BUILDTOOL}_modules_worker" "${codebase}" compile
    modules_messages "${codebase}" compile true

    for plugin in ${PLUGINS}; do
      verify_patchdir_still_exists
      if declare -f ${plugin}_compile >/dev/null 2>&1; then
        yetus_debug "Running ${plugin}_compile ${codebase} ${multijdkmode}"
        "${plugin}_compile" "${codebase}" "${multijdkmode}"
        ((result = result + $?))
      fi
    done

  done
  JAVA_HOME=${savejavahome}

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Execute the static analysis test cycle.
## @description  This will callout to _precompile, compile, _postcompile and _rebuild
## @audience     public
## @stability    evolving
## @replaceable  no
## @param        branch|patch
## @return       0 on success
## @return       1 on failure
function compile_cycle
{
  declare codebase=$1
  declare result=0
  declare plugin

  find_changed_modules

  for plugin in ${PROJECT_NAME} ${BUILDTOOL} ${PLUGINS} ${TESTFORMATS}; do
    if declare -f ${plugin}_precompile >/dev/null 2>&1; then
      yetus_debug "Running ${plugin}_precompile"
      #shellcheck disable=SC2086
      ${plugin}_precompile ${codebase}
      if [[ $? -gt 0 ]]; then
        ((result = result+1))
      fi
    fi
  done

  compile "${codebase}"

  for plugin in ${PROJECT_NAME} ${BUILDTOOL} ${PLUGINS} ${TESTFORMATS}; do
    if declare -f ${plugin}_postcompile >/dev/null 2>&1; then
      yetus_debug "Running ${plugin}_postcompile"
      #shellcheck disable=SC2086
      ${plugin}_postcompile ${codebase}
      if [[ $? -gt 0 ]]; then
        ((result = result+1))
      fi
    fi
  done

  for plugin in ${PROJECT_NAME} ${BUILDTOOL} ${PLUGINS} ${TESTFORMATS}; do
    if declare -f ${plugin}_rebuild >/dev/null 2>&1; then
      yetus_debug "Running ${plugin}_rebuild"
      #shellcheck disable=SC2086
      ${plugin}_rebuild ${codebase}
      if [[ $? -gt 0 ]]; then
        ((result = result+1))
      fi
    fi
  done

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Execute the patch file test phase. Calls out to
## @description  to _patchfile
## @audience     public
## @stability    evolving
## @replaceable  no
## @param        branch|patch
## @return       0 on success
## @return       1 on failure
function patchfiletests
{
  declare plugin
  declare result=0

  for plugin in ${BUILDTOOL} ${PLUGINS} ${TESTFORMATS}; do
    if declare -f ${plugin}_patchfile >/dev/null 2>&1; then
      yetus_debug "Running ${plugin}_patchfile"
      #shellcheck disable=SC2086
      ${plugin}_patchfile "${PATCH_DIR}/patch"
      if [[ $? -gt 0 ]]; then
        ((result = result+1))
      fi
    fi
  done

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}


## @description  Wipe the repo clean to not invalidate tests
## @audience     public
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function distclean
{
  declare result=0
  declare plugin

  personality_modules branch distclean

  for plugin in ${PLUGINS} ${TESTFORMATS}; do
    if declare -f ${plugin}_clean >/dev/null 2>&1; then
      yetus_debug "Running ${plugin}_distclean"
      #shellcheck disable=SC2086
      ${plugin}_clean
      if [[ $? -gt 0 ]]; then
        ((result = result+1))
      fi
    fi
  done

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Setup to execute
## @audience     public
## @stability    evolving
## @replaceable  no
## @param        $@
## @return       0 on success
## @return       1 on failure
function initialize
{
  setup_defaults

  parse_args "$@"

  importplugins

  parse_args_plugins "$@"

  plugins_initialize

  finish_docker_stats

  locate_patch

  # from here on out, we'll be in ${BASEDIR} for cwd
  # plugins need to pushd/popd if they change.
  git_checkout
  RESULT=$?
  if [[ ${JENKINS} == "true" ]] ; then
    if [[ ${RESULT} != 0 ]] ; then
      exit 1
    fi
  fi

  find_changed_files

  check_reexec

  determine_needed_tests

  prepopulate_footer
}

## @description perform prechecks
## @audience private
## @stability evolving
## @return   exits on failure
function prechecks
{
  declare plugin
  declare result=0

  verify_patch_file
  (( result = result + $? ))
  if [[ ${result} != 0 ]] ; then
    bugsystem_finalreport 1
    cleanup_and_exit 1
  fi

  for plugin in ${BUILDTOOL} ${PLUGINS} ${TESTFORMATS}; do
    verify_patchdir_still_exists

    if declare -f ${plugin}_precheck >/dev/null 2>&1; then

      yetus_debug "Running ${plugin}_precheck"
      #shellcheck disable=SC2086
      ${plugin}_precheck

      (( result = result + $? ))
      if [[ ${result} != 0 ]] ; then
        bugsystem_finalreport 1
        cleanup_and_exit 1
      fi
    fi
  done
}

###############################################################################
###############################################################################
###############################################################################

initialize "$@"

prechecks

patchfiletests
((RESULT=RESULT+$?))

compile_cycle branch
((RESULT=RESULT+$?))

distclean

apply_patch_file

compute_gitdiff

compile_cycle patch
((RESULT=RESULT+$?))

runtests
((RESULT=RESULT+$?))

finish_vote_table

finish_footer_table

bugsystem_finalreport ${RESULT}
cleanup_and_exit ${RESULT}
