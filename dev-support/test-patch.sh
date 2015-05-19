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

### BUILD_URL is set by Hudson if it is run by patch process

this="${BASH_SOURCE-$0}"
BINDIR=$(cd -P -- "$(dirname -- "${this}")" >/dev/null && pwd -P)
CWD=$(pwd)
USER_PARAMS=("$@")
GLOBALTIMER=$(date +"%s")

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

  PROJECT_NAME=hadoop
  HOW_TO_CONTRIBUTE="https://wiki.apache.org/hadoop/HowToContribute"
  JENKINS=false
  BASEDIR=$(pwd)
  RELOCATE_PATCH_DIR=false

  USER_PLUGIN_DIR=""
  LOAD_SYSTEM_PLUGINS=true

  FINDBUGS_HOME=${FINDBUGS_HOME:-}
  ECLIPSE_HOME=${ECLIPSE_HOME:-}
  BUILD_NATIVE=${BUILD_NATIVE:-true}
  PATCH_BRANCH=""
  PATCH_BRANCH_DEFAULT="trunk"
  CHANGED_MODULES=""
  USER_MODULE_LIST=""
  OFFLINE=false
  CHANGED_FILES=""
  REEXECED=false
  RESETREPO=false
  ISSUE=""
  ISSUE_RE='^(HADOOP|YARN|MAPREDUCE|HDFS)-[0-9]+$'
  TIMER=$(date +"%s")
  PATCHURL=""

  OSTYPE=$(uname -s)

  # Solaris needs POSIX, not SVID
  case ${OSTYPE} in
    SunOS)
      PS=${PS:-ps}
      AWK=${AWK:-/usr/xpg4/bin/awk}
      SED=${SED:-/usr/xpg4/bin/sed}
      WGET=${WGET:-wget}
      GIT=${GIT:-git}
      EGREP=${EGREP:-/usr/xpg4/bin/egrep}
      GREP=${GREP:-/usr/xpg4/bin/grep}
      PATCH=${PATCH:-patch}
      DIFF=${DIFF:-/usr/gnu/bin/diff}
      JIRACLI=${JIRA:-jira}
      FILE=${FILE:-file}
    ;;
    *)
      PS=${PS:-ps}
      AWK=${AWK:-awk}
      SED=${SED:-sed}
      WGET=${WGET:-wget}
      GIT=${GIT:-git}
      EGREP=${EGREP:-egrep}
      GREP=${GREP:-grep}
      PATCH=${PATCH:-patch}
      DIFF=${DIFF:-diff}
      JIRACLI=${JIRA:-jira}
      FILE=${FILE:-file}
    ;;
  esac

  declare -a JIRA_COMMENT_TABLE
  declare -a JIRA_FOOTER_TABLE
  declare -a JIRA_HEADER
  declare -a JIRA_TEST_TABLE

  JFC=0
  JTC=0
  JTT=0
  RESULT=0
}

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
    echo "[$(date) DEBUG]: $*" 1>&2
  fi
}

## @description  Activate the local timer
## @audience     public
## @stability    stable
## @replaceable  no
function start_clock
{
  hadoop_debug "Start clock"
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
  hadoop_debug "Stop clock"

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
  hadoop_debug "Stop global clock"

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
function add_jira_header
{
  JIRA_HEADER[${JHC}]="| $* |"
  JHC=$(( JHC+1 ))
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
function add_jira_table
{
  local value=$1
  local subsystem=$2
  shift 2

  local color
  local calctime=0

  local -r elapsed=$(stop_clock)

  if [[ ${elapsed} -lt 0 ]]; then
    calctime="N/A"
  else
    printf -v calctime "%3sm %02ss" $((elapsed/60)) $((elapsed%60))
  fi

  echo ""
  echo "Elapsed time: ${calctime}"
  echo ""

  case ${value} in
    1|+1)
      value="+1"
      color="green"
    ;;
    -1)
      color="red"
    ;;
    0)
      color="blue"
    ;;
    null)
    ;;
  esac

  if [[ -z ${color} ]]; then
    JIRA_COMMENT_TABLE[${JTC}]="|  | ${subsystem} | | ${*:-} |"
    JTC=$(( JTC+1 ))
  else
    JIRA_COMMENT_TABLE[${JTC}]="| {color:${color}}${value}{color} | ${subsystem} | ${calctime} | $* |"
    JTC=$(( JTC+1 ))
  fi
}

## @description  Put the final environment information at the bottom
## @description  of the footer table
## @stability     stable
## @audience     private
## @replaceable  yes
function close_jira_footer
{
  # shellcheck disable=SC2016
  local -r javaversion=$("${JAVA_HOME}/bin/java" -version 2>&1 | head -1 | ${AWK} '{print $NF}' | tr -d \")
  local -r unamea=$(uname -a)

  add_jira_footer "Java" "${javaversion}"
  add_jira_footer "uname" "${unamea}"
}

## @description  Put the final elapsed time at the bottom of the table.
## @audience     private
## @stability    stable
## @replaceable  no
function close_jira_table
{

  local -r elapsed=$(stop_global_clock)

  if [[ ${elapsed} -lt 0 ]]; then
    calctime="N/A"
  else
    printf -v calctime "%3sm %02ss" $((elapsed/60)) $((elapsed%60))
  fi

  echo ""
  echo "Total Elapsed time: ${calctime}"
  echo ""


  JIRA_COMMENT_TABLE[${JTC}]="| | | ${calctime} | |"
  JTC=$(( JTC+1 ))
}

## @description  Add to the footer of the display. @@BASE@@ will get replaced with the
## @description  correct location for the local filesystem in dev mode or the URL for
## @description  Jenkins mode.
## @audience     public
## @stability    stable
## @replaceable  no
## @param        subsystem
## @param        string
function add_jira_footer
{
  local subsystem=$1
  shift 1

  JIRA_FOOTER_TABLE[${JFC}]="| ${subsystem} | $* |"
  JFC=$(( JFC+1 ))
}

## @description  Special table just for unit test failures
## @audience     public
## @stability    stable
## @replaceable  no
## @param        failurereason
## @param        testlist
function add_jira_test_table
{
  local failure=$1
  shift 1

  JIRA_TEST_TABLE[${JTT}]="| ${failure} | $* |"
  JTT=$(( JTT+1 ))
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

## @description  Remove {color} tags from a string
## @audience     public
## @stability    stable
## @replaceable  no
## @param        string
## @return       string
function colorstripper
{
  local string=$1
  shift 1

  local green=""
  local white=""
  local red=""
  local blue=""

  echo "${string}" | \
  ${SED} -e "s,{color:red},${red},g" \
         -e "s,{color:green},${green},g" \
         -e "s,{color:blue},${blue},g" \
         -e "s,{color},${white},g"
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

  until [[ ${i} -gt ${sizeofa} ]]; do
    # shellcheck disable=SC2086
    string=$( echo ${a[$i]} | cut -f$((column + 1)) -d\| )
    if [[ ${#string} -gt $maxlen ]]; then
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
    case $(uname -s) in
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
    add_jira_table -1 pre-patch "JAVA_HOME is not defined."
    return 1
  fi
  return 0
}

## @description Write the contents of a file to jenkins
## @params filename
## @stability stable
## @audience public
## @returns ${JIRACLI} exit code
function write_to_jira
{
  local -r commentfile=${1}
  shift

  local retval

  if [[ ${OFFLINE} == false
     && ${JENKINS} == true ]]; then
    export USER=hudson
    # shellcheck disable=SC2086
    ${JIRACLI} --comment "$(cat ${commentfile})" \
               -s https://issues.apache.org/jira \
               -a addcomment -u hadoopqa \
               -p "${JIRA_PASSWD}" \
               --issue "${ISSUE}"
    retval=$?
    ${JIRACLI} -s https://issues.apache.org/jira \
               -a logout -u hadoopqa \
               -p "${JIRA_PASSWD}"
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

        write_to_jira ${commentfile}
      fi

      rm "${commentfile}"
      cleanup_and_exit ${RESULT}
    fi
}

## @description generate a list of all files and line numbers that
## @description that were added/changed in the source repo
## @audience    private
## @stability   stable
## @params      filename
## @replaceable no
function compute_gitdiff
{
  local outfile=$1
  local file
  local line
  local startline
  local counter
  local numlines
  local actual

  pushd "${BASEDIR}" >/dev/null
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
      until [[ ${counter} -gt ${numlines} ]]; do
          ((actual=counter+startline))
          echo "${file}:${actual}:" >> "${outfile}"
          ((counter=counter+1))
      done
    fi
  done < <("${GIT}" diff --unified=0 --no-color)
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
  echo "${*} > ${logfile} 2>&1"
  "${@}" > "${logfile}" 2>&1
}

## @description is PATCH_DIR relative to BASEDIR?
## @audience    public
## @stability   stable
## @replaceable yes
## @returns     1 - no, PATCH_DIR
## @returns     0 - yes, PATCH_DIR - BASEDIR
function relative_patchdir
{
  local p=${PATCH_DIR#${BASEDIR}}

  if [[ ${#p} -eq ${#PATCH_DIR} ]]; then
    echo ${p}
    return 1
  fi
  p=${p#/}
  echo ${p}
  return 0
}


## @description  Print the usage information
## @audience     public
## @stability    stable
## @replaceable  no
function hadoop_usage
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
  echo "--branch-default=<ref> If the branch isn't forced and we don't detect one in the patch name, use this branch (default 'trunk')"
  echo "--build-native=<bool>  If true, then build native components (default 'true')"
  echo "--contrib-guide=<url>  URL to point new users towards project conventions. (default Hadoop's wiki)"
  echo "--debug                If set, then output some extra stuff to stderr"
  echo "--dirty-workspace      Allow the local git workspace to have uncommitted changes"
  echo "--findbugs-home=<path> Findbugs home directory (default FINDBUGS_HOME environment variable)"
  echo "--issue-re=<expr>      Bash regular expression to use when trying to find a jira ref in the patch name (default '^(HADOOP|YARN|MAPREDUCE|HDFS)-[0-9]+$')"
  echo "--modulelist=<list>    Specify additional modules to test (comma delimited)"
  echo "--offline              Avoid connecting to the Internet"
  echo "--patch-dir=<dir>      The directory for working and output files (default '/tmp/${PROJECT_NAME}-test-patch/pid')"
  echo "--plugins=<dir>        A directory of user provided plugins. see test-patch.d for examples (default empty)"
  echo "--project=<name>       The short name for project currently using test-patch (default 'hadoop')"
  echo "--resetrepo            Forcibly clean the repo"
  echo "--run-tests            Run all relevant tests below the base directory"
  echo "--skip-system-plugins  Do not load plugins from ${BINDIR}/test-patch.d"
  echo "--testlist=<list>      Specify which subsystem tests to use (comma delimited)"

  echo "Shell binary overrides:"
  echo "--awk-cmd=<cmd>        The 'awk' command to use (default 'awk')"
  echo "--diff-cmd=<cmd>       The GNU-compatible 'diff' command to use (default 'diff')"
  echo "--file-cmd=<cmd>       The 'file' command to use (default 'file')"
  echo "--git-cmd=<cmd>        The 'git' command to use (default 'git')"
  echo "--grep-cmd=<cmd>       The 'grep' command to use (default 'grep')"
  echo "--mvn-cmd=<cmd>        The 'mvn' command to use (default \${MAVEN_HOME}/bin/mvn, or 'mvn')"
  echo "--patch-cmd=<cmd>      The 'patch' command to use (default 'patch')"
  echo "--ps-cmd=<cmd>         The 'ps' command to use (default 'ps')"
  echo "--sed-cmd=<cmd>        The 'sed' command to use (default 'sed')"

  echo
  echo "Jenkins-only options:"
  echo "--jenkins              Run by Jenkins (runs tests and posts results to JIRA)"
  echo "--eclipse-home=<path>  Eclipse home directory (default ECLIPSE_HOME environment variable)"
  echo "--jira-cmd=<cmd>       The 'jira' command to use (default 'jira')"
  echo "--jira-password=<pw>   The password for the 'jira' command"
  echo "--mv-patch-dir         Move the patch-dir into the basedir during cleanup."
  echo "--wget-cmd=<cmd>       The 'wget' command to use (default 'wget')"
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
      --build-native=*)
        BUILD_NATIVE=${i#*=}
      ;;
      --contrib-guide=*)
        HOW_TO_CONTRIBUTE=${i#*=}
      ;;
      --debug)
        HADOOP_SHELL_SCRIPT_DEBUG=true
      ;;
      --diff-cmd=*)
        DIFF=${i#*=}
      ;;
      --dirty-workspace)
        DIRTY_WORKSPACE=true
      ;;
      --eclipse-home=*)
        ECLIPSE_HOME=${i#*=}
      ;;
      --file-cmd=*)
        FILE=${i#*=}
      ;;
      --findbugs-home=*)
        FINDBUGS_HOME=${i#*=}
      ;;
      --git-cmd=*)
        GIT=${i#*=}
      ;;
      --grep-cmd=*)
        GREP=${i#*=}
      ;;
      --help|-help|-h|help|--h|--\?|-\?|\?)
        hadoop_usage
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
      ;;
      --jira-cmd=*)
        JIRACLI=${i#*=}
      ;;
      --jira-password=*)
        JIRA_PASSWD=${i#*=}
      ;;
      --modulelist=*)
        USER_MODULE_LIST=${i#*=}
        USER_MODULE_LIST=${USER_MODULE_LIST//,/ }
        hadoop_debug "Manually forcing modules ${USER_MODULE_LIST}"
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
      --plugins=*)
        USER_PLUGIN_DIR=${i#*=}
      ;;
      --project=*)
        PROJECT_NAME=${i#*=}
      ;;
      --ps-cmd=*)
        PS=${i#*=}
      ;;
      --reexec)
        REEXECED=true
        start_clock
        add_jira_table 0 reexec "dev-support patch detected."
      ;;
      --resetrepo)
        RESETREPO=true
      ;;
      --run-tests)
        RUN_TESTS=true
      ;;
      --skip-system-plugins)
        LOAD_SYSTEM_PLUGINS=false
      ;;
      --testlist=*)
        testlist=${i#*=}
        testlist=${testlist//,/ }
        for j in ${testlist}; do
          hadoop_debug "Manually adding patch test subsystem ${j}"
          add_test "${j}"
        done
      ;;
      --wget-cmd=*)
        WGET=${i#*=}
      ;;
      *)
        PATCH_OR_ISSUE=${i}
      ;;
    esac
  done

  # we need absolute dir for ${BASEDIR}
  cd "${CWD}"
  BASEDIR=$(cd -P -- "${BASEDIR}" >/dev/null && pwd -P)

  if [[ ${BUILD_NATIVE} == "true" ]] ; then
    NATIVE_PROFILE=-Pnative
    REQUIRE_TEST_LIB_HADOOP=-Drequire.test.libhadoop
  fi
  if [[ -z "${PATCH_OR_ISSUE}" ]]; then
    hadoop_usage
    exit 1
  fi
  if [[ ${JENKINS} == "true" ]] ; then
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

  if [[ -n ${USER_PATCH_DIR} ]]; then
    PATCH_DIR="${USER_PATCH_DIR}"
  else
    PATCH_DIR=/tmp/${PROJECT_NAME}-test-patch/$$
  fi

  cd "${CWD}"
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

  GITDIFFLINES=${PATCH_DIR}/gitdifflines.txt
}

## @description  Locate the pom.xml file for a given directory
## @audience     private
## @stability    stable
## @replaceable  no
## @return       directory containing the pom.xml
function find_pom_dir
{
  local dir

  dir=$(dirname "$1")

  hadoop_debug "Find pom dir for: ${dir}"

  while builtin true; do
    if [[ -f "${dir}/pom.xml" ]];then
      echo "${dir}"
      hadoop_debug "Found: ${dir}"
      return
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
  # Additionally, remove any a/ b/ patterns at the front
  # of the patch filenames and any revision info at the end
  # shellcheck disable=SC2016
  CHANGED_FILES=$(${GREP} -E '^(\+\+\+|---) ' "${PATCH_DIR}/patch" \
    | ${SED} \
      -e 's,^....,,' \
      -e 's,^[ab]/,,' \
    | ${GREP} -v /dev/null \
    | ${AWK} '{print $1}' \
    | sort -u)
}

## @description  Find the modules of the maven build that ${PATCH_DIR}/patch modifies
## @audience     private
## @stability    stable
## @replaceable  no
## @return       None; sets ${CHANGED_MODULES}
function find_changed_modules
{
  # Come up with a list of changed files into ${TMP}
  local pomdirs
  local module
  local pommods

  # Now find all the modules that were changed
  for file in ${CHANGED_FILES}; do
    #shellcheck disable=SC2086
    pomdirs="${pomdirs} $(find_pom_dir ${file})"
  done

  # Filter out modules without code
  for module in ${pomdirs}; do
    ${GREP} "<packaging>pom</packaging>" "${module}/pom.xml" > /dev/null
    if [[ "$?" != 0 ]]; then
      pommods="${pommods} ${module}"
    fi
  done

  #shellcheck disable=SC2086
  CHANGED_MODULES=$(echo ${pommods} ${USER_MODULE_LIST} | tr ' ' '\n' | sort -u)
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
    hadoop_error "ERROR: ${BASEDIR} is not a git repo."
    cleanup_and_exit 1
  fi

  if [[ ${RESETREPO} == "true" ]] ; then
    ${GIT} reset --hard
    if [[ $? != 0 ]]; then
      hadoop_error "ERROR: git reset is failing"
      cleanup_and_exit 1
    fi

    # if PATCH_DIR is in BASEDIR, then we don't want
    # git wiping it out.
    exemptdir=$(relative_patchdir)
    if [[ $? == 1 ]]; then
      ${GIT} clean -xdf
    else
      # we do, however, want it emptied of all _files_.
      # we need to leave _directories_ in case we are in
      # re-exec mode (which places a directory full of stuff in it)
      hadoop_debug "Exempting ${exemptdir} from clean"
      rm "${PATCH_DIR}/*" 2>/dev/null
      ${GIT} clean -xdf -e "${exemptdir}"
    fi
    if [[ $? != 0 ]]; then
      hadoop_error "ERROR: git clean is failing"
      cleanup_and_exit 1
    fi

    ${GIT} checkout --force "${PATCH_BRANCH_DEFAULT}"
    if [[ $? != 0 ]]; then
      hadoop_error "ERROR: git checkout --force ${PATCH_BRANCH_DEFAULT} is failing"
      cleanup_and_exit 1
    fi

    determine_branch
    if [[ ${PATCH_BRANCH} =~ ^git ]]; then
      PATCH_BRANCH=$(echo "${PATCH_BRANCH}" | cut -dt -f2)
    fi

    # we need to explicitly fetch in case the
    # git ref hasn't been brought in tree yet
    if [[ ${OFFLINE} == false ]]; then
      ${GIT} pull --rebase
      if [[ $? != 0 ]]; then
        hadoop_error "ERROR: git pull is failing"
        cleanup_and_exit 1
      fi
    fi
    # forcibly checkout this branch or git ref
    ${GIT} checkout --force "${PATCH_BRANCH}"
    if [[ $? != 0 ]]; then
      hadoop_error "ERROR: git checkout ${PATCH_BRANCH} is failing"
      cleanup_and_exit 1
    fi

    # if we've selected a feature branch that has new changes
    # since our last build, we'll need to rebase to see those changes.
    if [[ ${OFFLINE} == false ]]; then
      ${GIT} pull --rebase
      if [[ $? != 0 ]]; then
        hadoop_error "ERROR: git pull is failing"
        cleanup_and_exit 1
      fi
    fi

  else

    status=$(${GIT} status --porcelain)
    if [[ "${status}" != "" && -z ${DIRTY_WORKSPACE} ]] ; then
      hadoop_error "ERROR: --dirty-workspace option not provided."
      hadoop_error "ERROR: can't run in a workspace that contains the following modifications"
      hadoop_error "${status}"
      cleanup_and_exit 1
    fi

    determine_branch
    if [[ ${PATCH_BRANCH} =~ ^git ]]; then
      PATCH_BRANCH=$(echo "${PATCH_BRANCH}" | cut -dt -f2)
    fi

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

  add_jira_footer "git revision" "${PATCH_BRANCH} / ${GIT_REVISION}"

  if [[ ! -f ${BASEDIR}/pom.xml ]]; then
    hadoop_error "ERROR: This verison of test-patch.sh only supports Maven-based builds. Aborting."
    add_jira_table -1 pre-patch "Unsupported build system."
    output_to_jira 1
    cleanup_and_exit 1
  fi
  return 0
}

## @description  Confirm the source environment is compilable
## @audience     private
## @stability    stable
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function precheck_without_patch
{
  local -r mypwd=$(pwd)

  big_console_header "Pre-patch ${PATCH_BRANCH} Java verification"

  start_clock

  verify_needed_test javac

  if [[ $? == 1 ]]; then
    echo "Compiling ${mypwd}"
    echo_and_redirect "${PATCH_DIR}/${PATCH_BRANCH}JavacWarnings.txt" "${MVN}" clean test -DskipTests -D${PROJECT_NAME}PatchProcess -Ptest-patch
    if [[ $? != 0 ]] ; then
      echo "${PATCH_BRANCH} compilation is broken?"
      add_jira_table -1 pre-patch "${PATCH_BRANCH} compilation may be broken."
      return 1
    fi
  else
    echo "Patch does not appear to need javac tests."
  fi

  verify_needed_test javadoc

  if [[ $? == 1 ]]; then
    echo "Javadoc'ing ${mypwd}"
    echo_and_redirect "${PATCH_DIR}/${PATCH_BRANCH}JavadocWarnings.txt" "${MVN}" clean test javadoc:javadoc -DskipTests -Pdocs -D${PROJECT_NAME}PatchProcess
    if [[ $? != 0 ]] ; then
      echo "Pre-patch ${PATCH_BRANCH} javadoc compilation is broken?"
      add_jira_table -1 pre-patch "Pre-patch ${PATCH_BRANCH} JavaDoc compilation may be broken."
      return 1
    fi
  else
    echo "Patch does not appear to need javadoc tests."
  fi

  verify_needed_test site

  if [[ $? == 1 ]]; then
    echo "site creation for ${mypwd}"
    echo_and_redirect "${PATCH_DIR}/${PATCH_BRANCH}SiteWarnings.txt" "${MVN}" clean site site:stage -DskipTests -Dmaven.javadoc.skip=true -D${PROJECT_NAME}PatchProcess
    if [[ $? != 0 ]] ; then
      echo "Pre-patch ${PATCH_BRANCH} site compilation is broken?"
      add_jira_table -1 pre-patch "Pre-patch ${PATCH_BRANCH} site compilation may be broken."
      return 1
    fi
  else
    echo "Patch does not appear to need site tests."
  fi

  add_jira_table 0 pre-patch "Pre-patch ${PATCH_BRANCH} compilation is healthy."
  return 0
}

## @description  Confirm the given branch is a member of the list of space
## @description  delimited branches or a git ref
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        branch
## @param        branchlist
## @return       0 on success
## @return       1 on failure
function verify_valid_branch
{
  local branches=$1
  local check=$2
  local i

  # shortcut some common
  # non-resolvable names
  if [[ -z ${check} ]]; then
    return 1
  fi

  if [[ ${check} == patch ]]; then
    return 1
  fi

  if [[ ${check} =~ ^git ]]; then
    ref=$(echo "${check}" | cut -f2 -dt)
    count=$(echo "${ref}" | wc -c | tr -d ' ')

    if [[ ${count} == 8 || ${count} == 41 ]]; then
      return 0
    fi
    return 1
  fi

  for i in ${branches}; do
    if [[ "${i}" == "${check}" ]]; then
      return 0
    fi
  done
  return 1
}

## @description  Try to guess the branch being tested using a variety of heuristics
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success, with PATCH_BRANCH updated appropriately
## @return       1 on failure, with PATCH_BRANCH updated to PATCH_BRANCH_DEFAULT
function determine_branch
{
  local allbranches
  local patchnamechunk

  hadoop_debug "Determine branch"

  # something has already set this, so move on
  if [[ -n ${PATCH_BRANCH} ]]; then
    return
  fi

  pushd "${BASEDIR}" > /dev/null

  # developer mode, existing checkout, whatever
  if [[ "${DIRTY_WORKSPACE}" == true ]];then
    PATCH_BRANCH=$(${GIT} rev-parse --abbrev-ref HEAD)
    echo "dirty workspace mode; applying against existing branch"
    return
  fi

  allbranches=$(${GIT} branch -r | tr -d ' ' | ${SED} -e s,origin/,,g)

  for j in "${PATCHURL}" "${PATCH_OR_ISSUE}"; do
    hadoop_debug "Determine branch: starting with ${j}"
    # shellcheck disable=SC2016
    patchnamechunk=$(echo "${j}" | ${AWK} -F/ '{print $NF}')

    # ISSUE.branch.##.patch
    hadoop_debug "Determine branch: ISSUE.branch.##.patch"
    PATCH_BRANCH=$(echo "${patchnamechunk}" | cut -f2 -d. )
    verify_valid_branch "${allbranches}" "${PATCH_BRANCH}"
    if [[ $? == 0 ]]; then
      return
    fi

    # ISSUE-branch-##.patch
    hadoop_debug "Determine branch: ISSUE-branch-##.patch"
    PATCH_BRANCH=$(echo "${patchnamechunk}" | cut -f3- -d- | cut -f1,2 -d-)
    verify_valid_branch "${allbranches}" "${PATCH_BRANCH}"
    if [[ $? == 0 ]]; then
      return
    fi

    # ISSUE-##.patch.branch
    hadoop_debug "Determine branch: ISSUE-##.patch.branch"
    # shellcheck disable=SC2016
    PATCH_BRANCH=$(echo "${patchnamechunk}" | ${AWK} -F. '{print $NF}')
    verify_valid_branch "${allbranches}" "${PATCH_BRANCH}"
    if [[ $? == 0 ]]; then
      return
    fi

    # ISSUE-branch.##.patch
    hadoop_debug "Determine branch: ISSUE-branch.##.patch"
    # shellcheck disable=SC2016
    PATCH_BRANCH=$(echo "${patchnamechunk}" | cut -f3- -d- | ${AWK} -F. '{print $(NF-2)}' 2>/dev/null)
    verify_valid_branch "${allbranches}" "${PATCH_BRANCH}"
    if [[ $? == 0 ]]; then
      return
    fi
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

  hadoop_debug "Determine issue"

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

  hadoop_debug "Testing against ${testname}"

  if [[ -z ${NEEDED_TESTS} ]]; then
    hadoop_debug "Setting tests to ${testname}"
    NEEDED_TESTS=${testname}
  elif [[ ! ${NEEDED_TESTS} =~ ${testname} ]] ; then
    hadoop_debug "Adding ${testname}"
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
    if [[ ${i} =~ src/main/webapp ]]; then
      hadoop_debug "tests/webapp: ${i}"
    elif [[ ${i} =~ \.sh
         || ${i} =~ \.cmd
         ]]; then
      hadoop_debug "tests/shell: ${i}"
    elif [[ ${i} =~ \.md$
         || ${i} =~ \.md\.vm$
         || ${i} =~ src/site
         || ${i} =~ src/main/docs
         ]]; then
      hadoop_debug "tests/site: ${i}"
      add_test site
    elif [[ ${i} =~ \.c$
         || ${i} =~ \.cc$
         || ${i} =~ \.h$
         || ${i} =~ \.hh$
         || ${i} =~ \.proto$
         || ${i} =~ src/test
         || ${i} =~ \.cmake$
         || ${i} =~ CMakeLists.txt
         ]]; then
      hadoop_debug "tests/units: ${i}"
      add_test javac
      add_test unit
    elif [[ ${i} =~ pom.xml$
         || ${i} =~ \.java$
         || ${i} =~ src/main
         ]]; then
      hadoop_debug "tests/javadoc+units: ${i}"
      add_test javadoc
      add_test javac
      add_test unit
    fi

    if [[ ${i} =~ \.java$ ]]; then
      add_test findbugs
    fi

    for plugin in ${PLUGINS}; do
      if declare -f ${plugin}_filefilter >/dev/null 2>&1; then
        "${plugin}_filefilter" "${i}"
      fi
    done
  done

  add_jira_footer "Optional Tests" "${NEEDED_TESTS}"
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
  hadoop_debug "locate patch"

  if [[ -f ${PATCH_OR_ISSUE} ]]; then
    PATCH_FILE="${PATCH_OR_ISSUE}"
  else
    if [[ ${PATCH_OR_ISSUE} =~ ^http ]]; then
      echo "Patch is being downloaded at $(date) from"
      PATCHURL="${PATCH_OR_ISSUE}"
    else
      ${WGET} -q -O "${PATCH_DIR}/jira" "http://issues.apache.org/jira/browse/${PATCH_OR_ISSUE}"

      if [[ $? != 0 ]];then
        hadoop_error "ERROR: Unable to determine what ${PATCH_OR_ISSUE} may reference."
        cleanup_and_exit 1
      fi

      if [[ $(${GREP} -c 'Patch Available' "${PATCH_DIR}/jira") == 0 ]] ; then
        if [[ ${JENKINS} == true ]]; then
          hadoop_error "ERROR: ${PATCH_OR_ISSUE} is not \"Patch Available\"."
          cleanup_and_exit 1
        else
          hadoop_error "WARNING: ${PATCH_OR_ISSUE} is not \"Patch Available\"."
        fi
      fi

      relativePatchURL=$(${GREP} -o '"/jira/secure/attachment/[0-9]*/[^"]*' "${PATCH_DIR}/jira" | ${GREP} -v -e 'htm[l]*$' | sort | tail -1 | ${GREP} -o '/jira/secure/attachment/[0-9]*/[^"]*')
      PATCHURL="http://issues.apache.org${relativePatchURL}"
      if [[ ! ${PATCHURL} =~ \.patch$ ]]; then
        notSureIfPatch=true
      fi
      patchNum=$(echo "${PATCHURL}" | ${GREP} -o '[0-9]*/' | ${GREP} -o '[0-9]*')
      echo "${ISSUE} patch is being downloaded at $(date) from"
    fi
    echo "${PATCHURL}"
    add_jira_footer "Patch URL" "${PATCHURL}"
    ${WGET} -q -O "${PATCH_DIR}/patch" "${PATCHURL}"
    if [[ $? != 0 ]];then
      hadoop_error "ERROR: ${PATCH_OR_ISSUE} could not be downloaded."
      cleanup_and_exit 1
    fi
    PATCH_FILE="${PATCH_DIR}/patch"
  fi

  if [[ ! -f "${PATCH_DIR}/patch" ]]; then
    cp "${PATCH_FILE}" "${PATCH_DIR}/patch"
    if [[ $? == 0 ]] ; then
      echo "Patch file ${PATCH_FILE} copied to ${PATCH_DIR}"
    else
      hadoop_error "ERROR: Could not copy ${PATCH_FILE} to ${PATCH_DIR}"
      cleanup_and_exit 1
    fi
  fi
  if [[ ${notSureIfPatch} == "true" ]]; then
    guess_patch_file "${PATCH_DIR}/patch"
    if [[ $? != 0 ]]; then
      hadoop_error "ERROR: ${PATCHURL} is not a patch file."
      cleanup_and_exit 1
    else
      hadoop_debug "The patch ${PATCHURL} was not named properly, but it looks like a patch file. proceeding, but issue/branch matching might go awry."
      add_jira_table 0 patch "The patch file was not named according to ${PROJECT_NAME}'s naming conventions. Please see ${HOW_TO_CONTRIBUTE} for instructions."
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

  hadoop_debug "Trying to guess is ${patch} is a patch file."
  fileOutput=$("${FILE}" "${patch}")
  if [[ $fileOutput =~ \ diff\  ]]; then
    hadoop_debug "file magic says it's a diff."
    return 0
  fi
  fileOutput=$(head -n 1 "${patch}" | "${EGREP}" "^(From [a-z0-9]* Mon Sep 17 00:00:00 2001)|(diff .*)|(Index: .*)$")
  if [[ $? == 0 ]]; then
    hadoop_debug "first line looks like a patch file."
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

  "${BINDIR}/smart-apply-patch.sh" "${PATCH_DIR}/patch" dryrun
  if [[ $? != 0 ]] ; then
    echo "PATCH APPLICATION FAILED"
    add_jira_table -1 patch "The patch command could not apply the patch during dryrun."
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
    add_jira_table -1 patch "The patch command could not apply the patch."
    output_to_console 1
    output_to_jira 1
    cleanup_and_exit 1
  fi
  return 0
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

  if [[ ${REEXECED} == true ]]; then
    big_console_header "Re-exec mode detected. Continuing."
    return
  fi

  if [[ ! ${CHANGED_FILES} =~ dev-support/test-patch
      || ${CHANGED_FILES} =~ dev-support/smart-apply ]] ; then
    return
  fi

  big_console_header "dev-support patch detected"

  if [[ ${RESETREPO} == false ]]; then
    ((RESULT = RESULT + 1))
    hadoop_debug "can't destructively change the working directory. run with '--resetrepo' please. :("
    add_jira_table -1 dev-support "Couldn't test dev-support changes because we aren't configured to destructively change the working directory."
    return
  fi

  printf "\n\nRe-executing against patched versions to test.\n\n"

  apply_patch_file

  if [[ ${JENKINS} == true ]]; then

    rm "${commentfile}" 2>/dev/null

    echo "(!) A patch to test-patch or smart-apply-patch has been detected. " > "${commentfile}"
    echo "Re-executing against the patched versions to perform further tests. " >> "${commentfile}"
    echo "The console is at ${BUILD_URL}console in case of problems." >> "${commentfile}"

    write_to_jira "${commentfile}"
    rm "${commentfile}"
  fi

  cd "${CWD}"
  mkdir -p "${PATCH_DIR}/dev-support-test"
  cp -pr "${BASEDIR}"/dev-support/test-patch* "${PATCH_DIR}/dev-support-test"
  cp -pr "${BASEDIR}"/dev-support/smart-apply* "${PATCH_DIR}/dev-support-test"

  big_console_header "exec'ing test-patch.sh now..."

  exec "${PATCH_DIR}/dev-support-test/test-patch.sh" \
    --reexec \
    --branch "${PATCH_BRANCH}" \
    --patch-dir="${PATCH_DIR}" \
      "${USER_PARAMS[@]}"
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

  big_console_header "Checking there are no @author tags in the patch."

  start_clock

  if [[ ${CHANGED_FILES} =~ dev-support/test-patch ]]; then
    add_jira_table 0 @author "Skipping @author checks as test-patch has been patched."
    return 0
  fi

  authorTags=$("${GREP}" -c -i '^[^-].*@author' "${PATCH_DIR}/patch")
  echo "There appear to be ${authorTags} @author tags in the patch."
  if [[ ${authorTags} != 0 ]] ; then
    add_jira_table -1 @author \
      "The patch appears to contain ${authorTags} @author tags which the Hadoop" \
      " community has agreed to not allow in code contributions."
    return 1
  fi
  add_jira_table +1 @author "The patch does not contain any @author tags."
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

  verify_needed_test unit

  if [[ $? == 0 ]]; then
    return 0
  fi

  big_console_header "Checking there are new or changed tests in the patch."

  start_clock

  for i in ${CHANGED_FILES}; do
    if [[ ${i} =~ /test/ ]]; then
      ((testReferences=testReferences + 1))
    fi
  done

  echo "There appear to be ${testReferences} test file(s) referenced in the patch."
  if [[ ${testReferences} == 0 ]] ; then
    add_jira_table -1 "tests included" \
      "The patch doesn't appear to include any new or modified tests. " \
      "Please justify why no new tests are needed for this patch." \
      "Also please list what manual steps were performed to verify this patch."
    return 1
  fi
  add_jira_table +1 "tests included" \
    "The patch appears to include ${testReferences} new or modified test files."
  return 0
}

## @description  Helper for check_javadoc
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function count_javadoc_warns
{
  local warningfile=$1

  #shellcheck disable=SC2016,SC2046
  return $(${EGREP} "^[0-9]+ warnings$" "${warningfile}" | ${AWK} '{sum+=$1} END {print sum}')
}

## @description  Count and compare the number of JavaDoc warnings pre- and post- patch
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function check_javadoc
{
  local numBranchJavadocWarnings
  local numPatchJavadocWarnings

  verify_needed_test javadoc

  if [[ $? == 0 ]]; then
    echo "This patch does not appear to need javadoc checks."
    return 0
  fi

  big_console_header "Determining number of patched javadoc warnings"

  start_clock

  if [[ -d hadoop-project ]]; then
    (cd hadoop-project; "${MVN}" install > /dev/null 2>&1)
  fi
  if [[ -d hadoop-common-project/hadoop-annotations ]]; then
    (cd hadoop-common-project/hadoop-annotations; "${MVN}" install > /dev/null 2>&1)
  fi
  echo_and_redirect "${PATCH_DIR}/patchJavadocWarnings.txt"  "${MVN}" clean test javadoc:javadoc -DskipTests -Pdocs -D${PROJECT_NAME}PatchProcess
  count_javadoc_warns "${PATCH_DIR}/${PATCH_BRANCH}JavadocWarnings.txt"
  numBranchJavadocWarnings=$?
  count_javadoc_warns "${PATCH_DIR}/patchJavadocWarnings.txt"
  numPatchJavadocWarnings=$?

  echo "There appear to be ${numBranchJavadocWarnings} javadoc warnings before the patch and ${numPatchJavadocWarnings} javadoc warnings after applying the patch."
  if [[ ${numBranchJavadocWarnings} != "" && ${numPatchJavadocWarnings} != "" ]] ; then
    if [[ ${numPatchJavadocWarnings} -gt ${numBranchJavadocWarnings} ]] ; then

      ${GREP} -i warning "${PATCH_DIR}/${PATCH_BRANCH}JavadocWarnings.txt" > "${PATCH_DIR}/${PATCH_BRANCH}JavadocWarningsFiltered.txt"
      ${GREP} -i warning "${PATCH_DIR}/patchJavadocWarnings.txt" > "${PATCH_DIR}/patchJavadocWarningsFiltered.txt"
      ${DIFF} -u "${PATCH_DIR}/${PATCH_BRANCH}JavadocWarningsFiltered.txt" \
        "${PATCH_DIR}/patchJavadocWarningsFiltered.txt" \
        > "${PATCH_DIR}/diffJavadocWarnings.txt"
      rm -f "${PATCH_DIR}/${PATCH_BRANCH}JavadocWarningsFiltered.txt" "${PATCH_DIR}/patchJavadocWarningsFiltered.txt"

      add_jira_table -1 javadoc "The applied patch generated "\
      "$((numPatchJavadocWarnings-numBranchJavadocWarnings))" \
      " additional warning messages."
      add_jira_footer javadoc "@@BASE@@/diffJavadocWarnings.txt"
      return 1
    fi
  fi
  add_jira_table +1 javadoc "There were no new javadoc warning messages."
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
  local -r mypwd=$(pwd)

  verify_needed_test site

  if [[ $? == 0 ]]; then
    echo "This patch does not appear to need site checks."
    return 0
  fi

  big_console_header "Determining if patched site still builds"

  start_clock

  echo "site creation for ${mypwd}"
  echo_and_redirect "${PATCH_DIR}/patchSiteWarnings.txt" "${MVN}" clean site site:stage -DskipTests -Dmaven.javadoc.skip=true -D${PROJECT_NAME}PatchProcess
  if [[ $? != 0 ]] ; then
    echo "Site compilation is broken"
    add_jira_table -1 site "Site compilation is broken."
    add_jira_footer site "@@BASE@@/patchSiteWarnings.txt"
    return 1
  fi
  add_jira_table +1 site "Site still builds."
  return 0
}

## @description  Helper for check_javac
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function count_javac_warns
{
  local warningfile=$1
  #shellcheck disable=SC2016,SC2046
  return $(${AWK} 'BEGIN {total = 0} {total += 1} END {print total}' "${warningfile}")
}

## @description  Count and compare the number of javac warnings pre- and post- patch
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function check_javac
{
  local branchJavacWarnings
  local patchJavacWarnings

  verify_needed_test javac

  if [[ $? == 0 ]]; then
    echo "This patch does not appear to need javac checks."
    return 0
  fi

  big_console_header "Determining number of patched javac warnings."

  start_clock

  echo_and_redirect "${PATCH_DIR}/patchJavacWarnings.txt" "${MVN}" clean test -DskipTests -D${PROJECT_NAME}PatchProcess ${NATIVE_PROFILE} -Ptest-patch
  if [[ $? != 0 ]] ; then
    add_jira_table -1 javac "The patch appears to cause the build to fail."
    return 2
  fi
  ### Compare ${PATCH_BRANCH} and patch javac warning numbers
  if [[ -f ${PATCH_DIR}/patchJavacWarnings.txt ]] ; then
    ${GREP} '\[WARNING\]' "${PATCH_DIR}/${PATCH_BRANCH}JavacWarnings.txt" > "${PATCH_DIR}/filtered${PATCH_BRANCH}JavacWarnings.txt"
    ${GREP} '\[WARNING\]' "${PATCH_DIR}/patchJavacWarnings.txt" > "${PATCH_DIR}/filteredPatchJavacWarnings.txt"

    count_javac_warns "${PATCH_DIR}/filtered${PATCH_BRANCH}JavacWarnings.txt"
    branchJavacWarnings=$?
    count_javac_warns "${PATCH_DIR}/filteredPatchJavacWarnings.txt"
    patchJavacWarnings=$?

    echo "There appear to be ${branchJavacWarnings} javac compiler warnings before the patch and ${patchJavacWarnings} javac compiler warnings after applying the patch."
    if [[ ${patchJavacWarnings} != "" && ${branchJavacWarnings} != "" ]] ; then
      if [[ ${patchJavacWarnings} -gt ${branchJavacWarnings} ]] ; then

        ${DIFF} "${PATCH_DIR}/filtered${PATCH_BRANCH}JavacWarnings.txt" \
        "${PATCH_DIR}/filteredPatchJavacWarnings.txt" \
        > "${PATCH_DIR}/diffJavacWarnings.txt"

        add_jira_table -1 javac "The applied patch generated "\
        "$((patchJavacWarnings-branchJavacWarnings))" \
        " additional warning messages."

        add_jira_footer javac "@@BASE@@/diffJavacWarnings.txt"

        return 1
      fi
    fi
  fi

  add_jira_table +1 javac "There were no new javac warning messages."
  return 0
}

## @description  Verify all files have an Apache License
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function check_apachelicense
{
  big_console_header "Determining number of patched release audit warnings."

  start_clock

  echo_and_redirect "${PATCH_DIR}/patchReleaseAuditOutput.txt" "${MVN}" apache-rat:check -D${PROJECT_NAME}PatchProcess
  #shellcheck disable=SC2038
  find "${BASEDIR}" -name rat.txt | xargs cat > "${PATCH_DIR}/patchReleaseAuditWarnings.txt"

  ### Compare ${PATCH_BRANCH} and patch release audit warning numbers
  if [[ -f ${PATCH_DIR}/patchReleaseAuditWarnings.txt ]] ; then
    patchReleaseAuditWarnings=$("${GREP}" -c '\!?????' "${PATCH_DIR}/patchReleaseAuditWarnings.txt")
    echo ""
    echo ""
    echo "There appear to be ${patchReleaseAuditWarnings} release audit warnings after applying the patch."
    if [[ ${patchReleaseAuditWarnings} != "" ]] ; then
      if [[ ${patchReleaseAuditWarnings} -gt 0 ]] ; then
        add_jira_table -1 "release audit" "The applied patch generated ${patchReleaseAuditWarnings} release audit warnings."

        ${GREP} '\!?????' "${PATCH_DIR}/patchReleaseAuditWarnings.txt" \
        >  "${PATCH_DIR}/patchReleaseAuditProblems.txt"

        echo "Lines that start with ????? in the release audit "\
            "report indicate files that do not have an Apache license header." \
            >> "${PATCH_DIR}/patchReleaseAuditProblems.txt"

        add_jira_footer "Release Audit" "@@BASE@@/patchReleaseAuditProblems.txt"

        return 1
      fi
    fi
  fi
  add_jira_table 1 "release audit" "The applied patch does not increase the total number of release audit warnings."
  return 0
}

## @description  Verify mvn install works
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function check_mvn_install
{
  local retval

  verify_needed_test javadoc
  retval=$?

  verify_needed_test javac
  ((retval = retval + $? ))

  if [[ ${retval} == 0 ]]; then
    echo "This patch does not appear to need mvn install checks."
    return 0
  fi

  big_console_header "Installing all of the jars"

  start_clock
  echo_and_redirect "${PATCH_DIR}/jarinstall.txt" "${MVN}" install -Dmaven.javadoc.skip=true -DskipTests -D${PROJECT_NAME}PatchProcess
  retval=$?
  if [[ ${retval} != 0 ]]; then
    add_jira_table -1 install "The patch causes mvn install to fail."
  else
    add_jira_table +1 install "mvn install still works."
  fi
  return ${retval}
}

## @description  Verify patch does not trigger any findbugs warnings
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function check_findbugs
{
  local findbugs_version
  local modules=${CHANGED_MODULES}
  local rc=0
  local module_suffix
  local findbugsWarnings=0
  local relative_file
  local newFindbugsWarnings
  local findbugsWarnings
  local line
  local firstpart
  local secondpart

  big_console_header "Determining number of patched Findbugs warnings."


  verify_needed_test findbugs
  if [[ $? == 0 ]]; then
    echo "Patch does not touch any java files. Skipping findbugs."
    return 0
  fi

  start_clock

  if [[ ! -e "${FINDBUGS_HOME}/bin/findbugs" ]]; then
    printf "\n\n%s is not executable.\n\n" "${FINDBUGS_HOME}/bin/findbugs"
    add_jira_table -1 findbugs "Findbugs is not installed."
    return 1
  fi

  for module in ${modules}
  do
    pushd "${module}" >/dev/null
    echo "  Running findbugs in ${module}"
    module_suffix=$(basename "${module}")
    echo_and_redirect "${PATCH_DIR}/patchFindBugsOutput${module_suffix}.txt" "${MVN}" clean test findbugs:findbugs -DskipTests -D${PROJECT_NAME}PatchProcess \
      < /dev/null
    (( rc = rc + $? ))
    popd >/dev/null
  done

  #shellcheck disable=SC2016
  findbugs_version=$(${AWK} 'match($0, /findbugs-maven-plugin:[^:]*:findbugs/) { print substr($0, RSTART + 22, RLENGTH - 31); exit }' "${PATCH_DIR}/patchFindBugsOutput${module_suffix}.txt")

  if [[ ${rc} -ne 0 ]]; then
    add_jira_table -1 findbugs "The patch appears to cause Findbugs (version ${findbugs_version}) to fail."
    return 1
  fi

  while read file
  do
    relative_file=${file#${BASEDIR}/} # strip leading ${BASEDIR} prefix
    if [[ ${relative_file} != "target/findbugsXml.xml" ]]; then
      module_suffix=${relative_file%/target/findbugsXml.xml} # strip trailing path
      module_suffix=$(basename "${module_suffix}")
    fi

    cp "${file}" "${PATCH_DIR}/patchFindbugsWarnings${module_suffix}.xml"

    "${FINDBUGS_HOME}/bin/setBugDatabaseInfo" -timestamp "01/01/2000" \
      "${PATCH_DIR}/patchFindbugsWarnings${module_suffix}.xml" \
      "${PATCH_DIR}/patchFindbugsWarnings${module_suffix}.xml"

    #shellcheck disable=SC2016
    newFindbugsWarnings=$("${FINDBUGS_HOME}/bin/filterBugs" \
      -first "01/01/2000" "${PATCH_DIR}/patchFindbugsWarnings${module_suffix}.xml" \
      "${PATCH_DIR}/newPatchFindbugsWarnings${module_suffix}.xml" \
      | ${AWK} '{print $1}')

    echo "Found $newFindbugsWarnings Findbugs warnings ($file)"

    findbugsWarnings=$((findbugsWarnings+newFindbugsWarnings))

    "${FINDBUGS_HOME}/bin/convertXmlToText" -html \
      "${PATCH_DIR}/newPatchFindbugsWarnings${module_suffix}.xml" \
      "${PATCH_DIR}/newPatchFindbugsWarnings${module_suffix}.html"

    if [[ ${newFindbugsWarnings} -gt 0 ]] ; then
      populate_test_table FindBugs "module:${module_suffix}"
      while read line; do
        firstpart=$(echo "${line}" | cut -f2 -d:)
        secondpart=$(echo "${line}" | cut -f9- -d' ')
        add_jira_test_table "" "${firstpart}:${secondpart}"
      done < <("${FINDBUGS_HOME}/bin/convertXmlToText" \
        "${PATCH_DIR}/newPatchFindbugsWarnings${module_suffix}.xml")

      add_jira_footer "Findbugs warnings" "@@BASE@@/newPatchFindbugsWarnings${module_suffix}.html"
    fi
  done < <(find "${BASEDIR}" -name findbugsXml.xml)

  if [[ ${findbugsWarnings} -gt 0 ]] ; then
    add_jira_table -1 findbugs "The patch appears to introduce ${findbugsWarnings} new Findbugs (version ${findbugs_version}) warnings."
    return 1
  fi

  add_jira_table +1 findbugs "The patch does not introduce any new Findbugs (version ${findbugs_version}) warnings."
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
  big_console_header "Running mvn eclipse:eclipse."

  verify_needed_test javac
  if [[ $? == 0 ]]; then
    echo "Patch does not touch any java files. Skipping mvn eclipse:eclipse"
    return 0
  fi

  start_clock

  echo_and_redirect "${PATCH_DIR}/patchEclipseOutput.txt" "${MVN}" eclipse:eclipse -D${PROJECT_NAME}PatchProcess
  if [[ $? != 0 ]] ; then
    add_jira_table -1 eclipse:eclipse "The patch failed to build with eclipse:eclipse."
    return 1
  fi
  add_jira_table +1 eclipse:eclipse "The patch built with eclipse:eclipse."
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
      add_jira_test_table "${reason}" "${i}"
      first="${reason}"
    else
      add_jira_test_table " " "${i}"
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
  verify_needed_test unit

  if [[ $? == 0 ]]; then
    echo "Existing unit tests do not test patched files. Skipping."
    return 0
  fi

  big_console_header "Running unit tests"

  start_clock

  local failed_tests=""
  local modules=${CHANGED_MODULES}
  local building_common=0
  local hdfs_modules
  local ordered_modules=""
  local failed_test_builds=""
  local test_timeouts=""
  local test_logfile
  local test_build_result
  local module_test_timeouts=""
  local result
  local totalresult=0
  local module_prefix

  #
  # If we are building hadoop-hdfs-project, we must build the native component
  # of hadoop-common-project first.  In order to accomplish this, we move the
  # hadoop-hdfs subprojects to the end of the list so that common will come
  # first.
  #
  # Of course, we may not be building hadoop-common at all-- in this case, we
  # explicitly insert a mvn compile -Pnative of common, to ensure that the
  # native libraries show up where we need them.
  #

  for module in ${modules}; do
    if [[ ${module} == hadoop-hdfs-project* ]]; then
      hdfs_modules="${hdfs_modules} ${module}"
    elif [[ ${module} == hadoop-common-project* ]]; then
      ordered_modules="${ordered_modules} ${module}"
      building_common=1
    else
      ordered_modules="${ordered_modules} ${module}"
    fi
  done

  if [[ -n "${hdfs_modules}" ]]; then
    ordered_modules="${ordered_modules} ${hdfs_modules}"
    if [[ ${building_common} -eq 0 ]]; then
      echo "  Building hadoop-common with -Pnative in order to provide libhadoop.so to the hadoop-hdfs unit tests."
      echo_and_redirect "${PATCH_DIR}/testrun_native.txt" "${MVN}" compile ${NATIVE_PROFILE} "-D${PROJECT_NAME}PatchProcess"
      if [[ $? != 0 ]]; then
        add_jira_table -1 "native" "Failed to build the native portion " \
          "of hadoop-common prior to running the unit tests in ${ordered_modules}"
        return 1
      else
        add_jira_table +1 "native" "Pre-build of native portion"
      fi
    fi
  fi

  for module in ${ordered_modules}; do
    result=0
    start_clock
    pushd "${module}" >/dev/null
    module_suffix=$(basename "${module}")
    module_prefix=$(echo "${module}" | cut -f2 -d- )

    test_logfile=${PATCH_DIR}/testrun_${module_suffix}.txt
    echo "  Running tests in ${module_suffix}"
    echo_and_redirect "${test_logfile}" "${MVN}" clean install -fae ${NATIVE_PROFILE} ${REQUIRE_TEST_LIB_HADOOP} -D${PROJECT_NAME}PatchProcess
    test_build_result=$?

    add_jira_footer "${module_suffix} test log" "@@BASE@@/testrun_${module_suffix}.txt"

    # shellcheck disable=2016
    module_test_timeouts=$(${AWK} '/^Running / { if (last) { print last } last=$2 } /^Tests run: / { last="" }' "${test_logfile}")
    if [[ -n "${module_test_timeouts}" ]] ; then
      test_timeouts="${test_timeouts} ${module_test_timeouts}"
      result=1
    fi

    #shellcheck disable=SC2026,SC2038,SC2016
    module_failed_tests=$(find . -name 'TEST*.xml'\
      | xargs "${GREP}" -l -E "<failure|<error"\
      | ${AWK} -F/ '{sub("TEST-org.apache.",""); sub(".xml",""); print $NF}')

    if [[ -n "${module_failed_tests}" ]] ; then
      failed_tests="${failed_tests} ${module_failed_tests}"
      result=1
    fi
    if [[ ${test_build_result} != 0 && -z "${module_failed_tests}" && -z "${module_test_timeouts}" ]] ; then
      failed_test_builds="${failed_test_builds} ${module_suffix}"
      result=1
    fi
    popd >/dev/null

    if [[ $result == 1 ]]; then
      add_jira_table -1 "${module_prefix} tests" "Tests failed in ${module_suffix}."
    else
      add_jira_table +1 "${module_prefix} tests" "Tests passed in ${module_suffix}."
    fi

    ((totalresult = totalresult + result))
  done

  if [[ -n "${failed_tests}" ]] ; then
    # shellcheck disable=SC2086
    populate_test_table "Failed unit tests" ${failed_tests}
  fi
  if [[ -n "${test_timeouts}" ]] ; then
    # shellcheck disable=SC2086
    populate_test_table "Timed out tests" ${test_timeouts}
  fi
  if [[ -n "${failed_test_builds}" ]] ; then
    # shellcheck disable=SC2086
    populate_test_table "Failed build" ${failed_test_builds}
  fi

  if [[ ${JENKINS} == true ]]; then
    add_jira_footer "Test Results" "${BUILD_URL}testReport/"
  fi

  if [[ ${totalresult} -gt 0 ]]; then
    return 1
  else
    return 0
  fi
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
  local i
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

  seccoladj=$(findlargest 2 "${JIRA_COMMENT_TABLE[@]}")
  if [[ ${seccoladj} -lt 10 ]]; then
    seccoladj=10
  fi

  seccoladj=$((seccoladj + 2 ))
  i=0
  until [[ $i -eq ${#JIRA_HEADER[@]} ]]; do
    printf "%s\n" "${JIRA_HEADER[${i}]}"
    ((i=i+1))
  done

  printf "| %s | %*s |  %s   | %s\n" "Vote" ${seccoladj} Subsystem Runtime "Comment"
  echo "============================================================================"
  i=0
  until [[ $i -eq ${#JIRA_COMMENT_TABLE[@]} ]]; do
    ourstring=$(echo "${JIRA_COMMENT_TABLE[${i}]}" | tr -s ' ')
    vote=$(echo "${ourstring}" | cut -f2 -d\|)
    vote=$(colorstripper "${vote}")
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

  if [[ ${#JIRA_TEST_TABLE[@]} -gt 0 ]]; then
    seccoladj=$(findlargest 1 "${JIRA_TEST_TABLE[@]}")
    printf "\n\n%*s | Tests\n" "${seccoladj}" "Reason"
    i=0
    until [[ $i -eq ${#JIRA_TEST_TABLE[@]} ]]; do
      ourstring=$(echo "${JIRA_TEST_TABLE[${i}]}" | tr -s ' ')
      vote=$(echo "${ourstring}" | cut -f2 -d\|)
      subs=$(echo "${ourstring}"  | cut -f3 -d\|)
      printf "%*s | %s\n" "${seccoladj}" "${vote}" "${subs}"
      ((i=i+1))
    done
  fi

  printf "\n\n|| Subsystem || Report/Notes ||\n"
  echo "============================================================================"
  i=0

  until [[ $i -eq ${#JIRA_FOOTER_TABLE[@]} ]]; do
    comment=$(echo "${JIRA_FOOTER_TABLE[${i}]}" |
              ${SED} -e "s,@@BASE@@,${PATCH_DIR},g")
    printf "%s\n" "${comment}"
    ((i=i+1))
  done
}

## @description  Print out the finished details to the JIRA issue
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        runresult
function output_to_jira
{
  local result=$1
  local i
  local commentfile=${PATCH_DIR}/commentfile
  local comment

  rm "${commentfile}" 2>/dev/null

  if [[ ${JENKINS} != "true" ]] ; then
    return 0
  fi

  big_console_header "Adding comment to JIRA"

  add_jira_footer "Console output" "${BUILD_URL}console"

  if [[ ${result} == 0 ]]; then
    add_jira_header "(/) *{color:green}+1 overall{color}*"
  else
    add_jira_header "(x) *{color:red}-1 overall{color}*"
  fi


  { echo "\\\\" ; echo "\\\\"; } >>  "${commentfile}"

  i=0
  until [[ $i -eq ${#JIRA_HEADER[@]} ]]; do
    printf "%s\n" "${JIRA_HEADER[${i}]}" >> "${commentfile}"
    ((i=i+1))
  done

  { echo "\\\\" ; echo "\\\\"; } >>  "${commentfile}"

  echo "|| Vote || Subsystem || Runtime || Comment ||" >> "${commentfile}"

  i=0
  until [[ $i -eq ${#JIRA_COMMENT_TABLE[@]} ]]; do
    printf "%s\n" "${JIRA_COMMENT_TABLE[${i}]}" >> "${commentfile}"
    ((i=i+1))
  done


  if [[ ${#JIRA_TEST_TABLE[@]} -gt 0 ]]; then
    { echo "\\\\" ; echo "\\\\"; } >>  "${commentfile}"

    echo "|| Reason || Tests ||" >>  "${commentfile}"
    i=0
    until [[ $i -eq ${#JIRA_TEST_TABLE[@]} ]]; do
      printf "%s\n" "${JIRA_TEST_TABLE[${i}]}" >> "${commentfile}"
      ((i=i+1))
    done
  fi

  { echo "\\\\" ; echo "\\\\"; } >>  "${commentfile}"

  echo "|| Subsystem || Report/Notes ||" >> "${commentfile}"
  i=0
  until [[ $i -eq ${#JIRA_FOOTER_TABLE[@]} ]]; do
    comment=$(echo "${JIRA_FOOTER_TABLE[${i}]}" |
              ${SED} -e "s,@@BASE@@,${BUILD_URL}artifact/patchprocess,g")
    printf "%s\n" "${comment}" >> "${commentfile}"
    ((i=i+1))
  done

  printf "\n\nThis message was automatically generated.\n\n" >> "${commentfile}"

  write_to_jira "${commentfile}"
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
    relative_patchdir >/dev/null
    if [[ $? == 1 ]]; then
      hadoop_debug "mv ${PATCH_DIR} ${BASEDIR}"
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

  for routine in find_java_home verify_patch_file
  do
    verify_patchdir_still_exists

    hadoop_debug "Running ${routine}"
    ${routine}

    (( RESULT = RESULT + $? ))
    if [[ ${RESULT} != 0 ]] ; then
      output_to_console 1
      output_to_jira 1
      cleanup_and_exit 1
    fi
  done

  for plugin in ${PLUGINS}; do
    verify_patchdir_still_exists

    if declare -f ${plugin}_postcheckout >/dev/null 2>&1; then

      hadoop_debug "Running ${plugin}_postcheckout"
      #shellcheck disable=SC2086
      ${plugin}_postcheckout

      (( RESULT = RESULT + $? ))
      if [[ ${RESULT} != 0 ]] ; then
        output_to_console 1
        output_to_jira 1
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

    hadoop_debug "Running ${routine}"
    ${routine}

    (( RESULT = RESULT + $? ))
  done

  for plugin in ${PLUGINS}; do
    verify_patchdir_still_exists

    if declare -f ${plugin}_preapply >/dev/null 2>&1; then

      hadoop_debug "Running ${plugin}_preapply"
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

  compute_gitdiff "${GITDIFFLINES}"

  check_javac
  retval=$?
  if [[ ${retval} -gt 1 ]] ; then
    output_to_console 1
    output_to_jira 1
    cleanup_and_exit 1
  fi

  ((RESULT = RESULT + retval))

  for routine in check_javadoc check_apachelicense check_site
  do
    verify_patchdir_still_exists
    hadoop_debug "Running ${routine}"
    $routine

    (( RESULT = RESULT + $? ))

  done

  for plugin in ${PLUGINS}; do
    verify_patchdir_still_exists
    if declare -f ${plugin}_postapply >/dev/null 2>&1; then
      hadoop_debug "Running ${plugin}_postapply"
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

  for routine in check_mvn_eclipse check_findbugs
  do
    verify_patchdir_still_exists
    hadoop_debug "Running ${routine}"
    ${routine}
    (( RESULT = RESULT + $? ))
  done

  for plugin in ${PLUGINS}; do
    verify_patchdir_still_exists
    if declare -f ${plugin}_postinstall >/dev/null 2>&1; then
      hadoop_debug "Running ${plugin}_postinstall"
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
      hadoop_debug "Running ${plugin}_tests"
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
    hadoop_debug "Loading user provided plugins from ${USER_PLUGIN_DIR}"
    files=("${files[@]}" ${USER_PLUGIN_DIR}/*.sh)
  fi

  for i in "${files[@]}"; do
    hadoop_debug "Importing ${i}"
    . "${i}"
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

###############################################################################
###############################################################################
###############################################################################

big_console_header "Bootstrapping test harness"

setup_defaults

parse_args "$@"

importplugins

locate_patch

find_changed_files

determine_needed_tests

# from here on out, we'll be in ${BASEDIR} for cwd
# routines need to pushd/popd if they change.
git_checkout
RESULT=$?
if [[ ${JENKINS} == "true" ]] ; then
  if [[ ${RESULT} != 0 ]] ; then
    exit 100
  fi
fi

check_reexec

postcheckout

find_changed_modules

preapply

apply_patch_file

postapply

check_mvn_install

postinstall

runtests

close_jira_footer

close_jira_table

output_to_console ${RESULT}
output_to_jira ${RESULT}
cleanup_and_exit ${RESULT}
