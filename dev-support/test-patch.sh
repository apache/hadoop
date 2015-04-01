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

### Setup some variables.
### BUILD_URL is set by Hudson if it is run by patch process
### Read variables from properties file
this="${BASH_SOURCE-$0}"
BINDIR=$(cd -P -- "$(dirname -- "${this}")" >/dev/null && pwd -P)

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
  JENKINS=false
  PATCH_DIR=/tmp/${PROJECT_NAME}-test-patch/$$
  SUPPORT_DIR=/tmp
  BASEDIR=$(pwd)

  FINDBUGS_HOME=${FINDBUGS_HOME:-}
  ECLIPSE_HOME=${ECLIPSE_HOME:-}
  BUILD_NATIVE=${BUILD_NATIVE:-true}
  PATCH_BRANCH=""
  CHANGED_MODULES=""
  ISSUE=""
  ISSUE_RE='^(HADOOP|YARN|MAPREDUCE|HDFS)-[0-9]+$'

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

  declare -a JIRA_COMMENT_TABLE
  declare -a JIRA_FOOTER_TABLE
  declare -a JIRA_HEADER
  declare -a JIRA_TEST_TABLE

  JFC=0
  JTC=0
  JTT=0
  RESULT=0
  TIMER=0
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

## @description  Activate the global timer
## @audience     public
## @stability    stable
## @replaceable  no
function start_clock
{
  hadoop_debug "Start clock"
  TIMER=$(date +"%s")
}

## @description  Print the elapsed time in seconds since the start of the global timer
## @audience     public
## @stability    stable
## @replaceable  no
function stop_clock
{
  local stoptime=$(date +"%s")
  local elapsed=$((stoptime-TIMER))
  hadoop_debug "Stop clock"

  echo ${elapsed}
}

## @description  Add time to the global timer
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

  local elapsed=$(stop_clock)

  if [[ ${elapsed} -lt 0 ]]; then
    calctime="N/A"
  else
    printf -v calctime "%02sm %02ss" $((elapsed/60)) $((elapsed%60))
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
  local spacing=$(( (70+${#text}) /2 ))
  printf "\n\n"
  echo "======================================================================="
  echo "======================================================================="
  printf "%*s\n"  ${spacing} "${text}"
  echo "======================================================================="
  echo "======================================================================="
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
  if [[ -z ${JAVA_HOME:-} ]]; then
    case $(uname -s) in
      Darwin)
        if [[ -z "${JAVA_HOME}" ]]; then
          if [[ -x /usr/libexec/java_home ]]; then
            export JAVA_HOME="$(/usr/libexec/java_home)"
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

## @description  Print the usage information
## @audience     public
## @stability    stable
## @replaceable  no
function hadoop_usage
{
  local up=$(echo ${PROJECT_NAME} | tr '[:lower:]' '[:upper:]')

  echo "Usage: test-patch.sh [options] patch-file | issue-number | http"
  echo
  echo "Where:"
  echo "  patch-file is a local patch file containing the changes to test"
  echo "  issue-number is a 'Patch Available' JIRA defect number (e.g. '${up}-9902') to test"
  echo "  http is an HTTP address to download the patch file"
  echo
  echo "Options:"
  echo "--basedir=<dir>        The directory to apply the patch to (default current directory)"
  echo "--build-native=<bool>  If true, then build native components (default 'true')"
  echo "--debug                If set, then output some extra stuff to stderr"
  echo "--dirty-workspace      Allow the local git workspace to have uncommitted changes"
  echo "--findbugs-home=<path> Findbugs home directory (default FINDBUGS_HOME environment variable)"
  echo "--patch-dir=<dir>      The directory for working and output files (default '/tmp/${PROJECT_NAME}-test-patch/pid')"
  echo "--run-tests            Run all tests below the base directory"

  echo "Shell binary overrides:"
  echo "--awk-cmd=<cmd>        The 'awk' command to use (default 'awk')"
  echo "--diff-cmd=<cmd>       The 'diff' command to use (default 'diff')"
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
  echo "--support-dir=<dir>    The directory to find support files in"
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

  for i in "$@"; do
    case ${i} in
      --java-home)
        JAVA_HOME=${i#*=}
      ;;
      --jenkins)
        JENKINS=true
      ;;
      --patch-dir=*)
        PATCH_DIR=${i#*=}
      ;;
      --support-dir=*)
        SUPPORT_DIR=${i#*=}
      ;;
      --basedir=*)
        BASEDIR=${i#*=}
      ;;
      --mvn-cmd=*)
        MVN=${i#*=}
      ;;
      --ps-cmd=*)
        PS=${i#*=}
      ;;
      --awk-cmd=*)
        AWK=${i#*=}
      ;;
      --wget-cmd=*)
        WGET=${i#*=}
      ;;
      --git-cmd=*)
        GIT=${i#*=}
      ;;
      --grep-cmd=*)
        GREP=${i#*=}
      ;;
      --patch-cmd=*)
        PATCH=${i#*=}
      ;;
      --diff-cmd=*)
        DIFF=${i#*=}
      ;;
      --jira-cmd=*)
        JIRACLI=${i#*=}
      ;;
      --jira-password=*)
        JIRA_PASSWD=${i#*=}
      ;;
      --findbugs-home=*)
        FINDBUGS_HOME=${i#*=}
      ;;
      --eclipse-home=*)
        ECLIPSE_HOME=${i#*=}
      ;;
      --dirty-workspace)
        DIRTY_WORKSPACE=true
      ;;
      --run-tests)
        RUN_TESTS=true
      ;;
      --debug)
        HADOOP_SHELL_SCRIPT_DEBUG=true
      ;;
      --build-native=*)
        BUILD_NATIVE=${i#*=}
      ;;
      *)
        PATCH_OR_ISSUE=${i}
      ;;
    esac
  done

  # if we get a relative path, turn it absolute
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
    # shellcheck disable=SC2034
    ECLIPSE_PROPERTY="-Declipse.home=${ECLIPSE_HOME}"
  else
    echo "Running in developer mode"
    JENKINS=false
  fi
  if [[ ! -d ${PATCH_DIR} ]]; then
    mkdir -p "${PATCH_DIR}"
    if [[ $? == 0 ]] ; then
      echo "${PATCH_DIR} has been created"
    else
      echo "Unable to create ${PATCH_DIR}"
      cleanup_and_exit 0
    fi
  else
    hadoop_error "WARNING: ${PATCH_DIR} already exists."
  fi
}

## @description  Locate the pom.xml file for a given directory
## @audience     private
## @stability    stable
## @replaceable  no
## @return       directory containing the pom.xml
function find_pom_dir
{
  local dir=$(dirname "$1")

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

## @description  Find the modules of the maven build that ${PATCH_DIR}/patch modifies
## @audience     private
## @stability    stable
## @replaceable  no
## @return       None; sets ${CHANGED_MODULES}
function find_changed_modules
{
  # Come up with a list of changed files into ${TMP}
  local tmp_paths="${PATCH_DIR}/tmp.paths.$$.tp"
  local tmp_modules="${PATCH_DIR}/tmp.modules.$$.tp"

  local module

  ${GREP} '^+++ \|^--- ' "${PATCH_DIR}/patch" | cut -c '5-' | ${GREP} -v /dev/null | sort -u > "${tmp_paths}"

  # if all of the lines start with a/ or b/, then this is a git patch that
  # was generated without --no-prefix
  if ! ${GREP} -qv '^a/\|^b/' "${tmp_paths}"; then
    ${SED} -i -e 's,^[ab]/,,' "${tmp_paths}"
  fi

  # Now find all the modules that were changed
  while read file; do
    find_pom_dir "${file}" >> "${tmp_modules}"
  done < <(cut -f1 "${tmp_paths}" | sort -u)
  rm "${tmp_paths}" 2>/dev/null

  # Filter out modules without code
  while read module; do
    ${GREP} "<packaging>pom</packaging>" "${module}/pom.xml" > /dev/null
    if [[ "$?" != 0 ]]; then
      CHANGED_MODULES="${CHANGED_MODULES} ${module}"
    fi
  done < <(sort -u "${tmp_modules}")
  rm "${tmp_modules}" 2>/dev/null
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

  big_console_header "Confirming git environment"

  if [[ ${JENKINS} == "true" ]] ; then
    cd "${BASEDIR}"
    ${GIT} reset --hard
    if [[ $? != 0 ]]; then
      hadoop_error "ERROR: git reset is failing"
      cleanup_and_exit 1
    fi
    ${GIT} clean -xdf
    if [[ $? != 0 ]]; then
      hadoop_error "ERROR: git clean is failing"
      cleanup_and_exit 1
    fi

    determine_branch

    ${GIT} checkout "${PATCH_BRANCH}"
    if [[ $? != 0 ]]; then
      hadoop_error "ERROR: git checkout ${PATCH_BRANCH} is failing"
      cleanup_and_exit 1
    fi
    ${GIT} pull --rebase
    if [[ $? != 0 ]]; then
      hadoop_error "ERROR: git pull is failing"
      cleanup_and_exit 1
    fi
    ### Copy in any supporting files needed by this process
    if [[ -d "${SUPPORT_DIR}"/lib ]]; then
      cp -r "${SUPPORT_DIR}"/lib/* ./lib
    fi
  else
    cd "${BASEDIR}"
    if [[ ! -d .git ]]; then
      hadoop_error "ERROR: ${BASEDIR} is not a git repo."
      cleanup_and_exit 1
    fi

    status=$(${GIT} status --porcelain)
    if [[ "${status}" != "" && -z ${DIRTY_WORKSPACE} ]] ; then
      hadoop_error "ERROR: --dirty-workspace option not provided."
      hadoop_error "ERROR: can't run in a workspace that contains the following modifications"
      hadoop_error "${status}"
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

  if [[ "${ISSUE}" = 'Unknown' ]]; then
    echo "Testing patch on ${PATCH_BRANCH}."
  else
    echo "Testing ${ISSUE} patch on ${PATCH_BRANCH}."
  fi

  add_jira_footer "git revision" "${GIT_REVISION}"
  return 0
}

## @description  Confirm the source environment is good, running prepatch counts of standard parts such
## @description  as javadoc.
## @audience     private
## @stability    stable
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function precheck_without_patch
{
  local mypwd=$(pwd)

  big_console_header "Pre-patch ${PATCH_BRANCH} verification"

  start_clock
  echo "Compiling ${mypwd}"
  if [[ -d "${mypwd}/hadoop-hdfs-project/hadoop-hdfs/target/test/data/dfs" ]]; then
    echo "Changing permission on ${mypwd}/hadoop-hdfs-project/hadoop-hdfs/target/test/data/dfs to avoid broken builds"
    chmod +x -R "${mypwd}/hadoop-hdfs-project/hadoop-hdfs/target/test/data/dfs"
  fi
  echo "${MVN} clean test -DskipTests -D${PROJECT_NAME}PatchProcess -Ptest-patch > ${PATCH_DIR}/${PATCH_BRANCH}JavacWarnings.txt 2>&1"
  ${MVN} clean test -DskipTests -D${PROJECT_NAME}PatchProcess -Ptest-patch > "${PATCH_DIR}/${PATCH_BRANCH}JavacWarnings.txt" 2>&1
  if [[ $? != 0 ]] ; then
    echo "${PATCH_BRANCH} compilation is broken?"
    add_jira_table -1 pre-patch "${PATCH_BRANCH} compilation may be broken."
    return 1
  fi

  echo "${MVN} clean test javadoc:javadoc -DskipTests -Pdocs -D${PROJECT_NAME}PatchProcess > ${PATCH_DIR}/${PATCH_BRANCH}JavadocWarnings.txt 2>&1"
  ${MVN} clean test javadoc:javadoc -DskipTests -Pdocs -D${PROJECT_NAME}PatchProcess > "${PATCH_DIR}/${PATCH_BRANCH}JavadocWarnings.txt" 2>&1
  if [[ $? != 0 ]] ; then
    echo "Pre-patch ${PATCH_BRANCH} javadoc compilation is broken?"
    add_jira_table -1 pre-patch "Pre-patch ${PATCH_BRANCH} JavaDoc compilation may be broken."
    return 1
  fi

  add_jira_table 0 pre-patch "Pre-patch ${PATCH_BRANCH} compilation is healthy."
  return 0
}

## @description  Confirm the given branch is a member of the list of space delimited branches.
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

  for i in ${branches}; do
    if [[ "${i}" = "${check}" ]]; then
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
## @return       1 on failure, with PATCH_BRANCH updated to "trunk"
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
  if [[ "${DIRTY_WORKSPACE}" = true ]];then
    PATCH_BRANCH=$(${GIT} rev-parse --abbrev-ref HEAD)
    echo "dirty workspace mode; applying against existing branch"
    return
  fi

  allbranches=$(${GIT} branch -r | tr -d ' ' | ${SED} -e s,origin/,,g)

  # shellcheck disable=SC2016
  patchnamechunk=$(echo "${PATCH_OR_ISSUE}" | ${AWK} -F/ '{print $NF}')

  # ISSUE.branch.##.patch
  PATCH_BRANCH=$(echo "${patchnamechunk}" | cut -f2 -d. )
  verify_valid_branch "${allbranches}" "${PATCH_BRANCH}"
  if [[ $? = 0 ]]; then
    return
  fi

  # ISSUE-branch-##.patch
  PATCH_BRANCH=$(echo "${patchnamechunk}" | cut -f3- -d- | cut -f1,2 -d-)
  verify_valid_branch "${allbranches}" "${PATCH_BRANCH}"
  if [[ $? = 0 ]]; then
    return
  fi

  # ISSUE-##.patch.branch
  # shellcheck disable=SC2016
  PATCH_BRANCH=$(echo "${patchnamechunk}" | ${AWK} -F. '{print $NF}')
  verify_valid_branch "${allbranches}" "${PATCH_BRANCH}"
  if [[ $? = 0 ]]; then
    return
  fi

  # ISSUE-branch.##.patch
  # shellcheck disable=SC2016
  PATCH_BRANCH=$(echo "${patchnamechunk}" | cut -f3- -d- | ${AWK} -F. '{print $(NF-2)}' 2>/dev/null)
  verify_valid_branch "${allbranches}" "${PATCH_BRANCH}"
  if [[ $? = 0 ]]; then
    return
  fi

  PATCH_BRANCH=trunk

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
  if [[ ${JENKINS} = true ]]; then
    ISSUE=${PATCH_OR_ISSUE}
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
## @description  Given ${PATCH_ISSUE}, determine what type of patch file is in use, and do the
## @description  necessary work to place it into ${PATCH_DIR}/patch.
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure, may exit
function locate_patch
{
  hadoop_debug "locate patch"

  if [[ -f ${PATCH_OR_ISSUE} ]]; then
    PATCH_FILE="${PATCH_OR_ISSUE}"
  else
    if [[ ${PATCH_OR_ISSUE} =~ ^http ]]; then
      echo "Patch is being downloaded at $(date) from"
      patchURL="${PATCH_OR_ISSUE}"
    else
      ${WGET} -q -O "${PATCH_DIR}/jira" "http://issues.apache.org/jira/browse/${PATCH_OR_ISSUE}"

      if [[ $? != 0 ]];then
        hadoop_error "Unable to determine what ${PATCH_OR_ISSUE} may reference."
        cleanup_and_exit 0
      fi

      if [[ $(${GREP} -c 'Patch Available' "${PATCH_DIR}/jira") == 0 ]] ; then
        hadoop_error "${PATCH_OR_ISSUE} is not \"Patch Available\".  Exiting."
        cleanup_and_exit 0
      fi

      relativePatchURL=$(${GREP} -o '"/jira/secure/attachment/[0-9]*/[^"]*' "${PATCH_DIR}/jira" | ${GREP} -v -e 'htm[l]*$' | sort | tail -1 | ${GREP} -o '/jira/secure/attachment/[0-9]*/[^"]*')
      patchURL="http://issues.apache.org${relativePatchURL}"
      patchNum=$(echo "${patchURL}" | ${GREP} -o '[0-9]*/' | ${GREP} -o '[0-9]*')
      echo "${ISSUE} patch is being downloaded at $(date) from"
    fi
    echo "${patchURL}"
    add_jira_footer "Patch URL" "${patchURL}"
    ${WGET} -q -O "${PATCH_DIR}/patch" "${patchURL}"
    if [[ $? != 0 ]];then
      hadoop_error "${PATCH_OR_ISSUE} could not be downloaded."
      cleanup_and_exit 0
    fi
    PATCH_FILE="${PATCH_DIR}/patch"
  fi

  if [[ ! -f "${PATCH_DIR}/patch" ]]; then
    cp "${PATCH_FILE}" "${PATCH_DIR}/patch"
    if [[ $? == 0 ]] ; then
      echo "Patch file ${PATCH_FILE} copied to ${PATCH_DIR}"
    else
      hadoop_error "Could not copy ${PATCH_FILE} to ${PATCH_DIR}"
      cleanup_and_exit 0
    fi
  fi
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

  authorTags=$("${GREP}" -c -i '@author' "${PATCH_DIR}/patch")
  echo "There appear to be ${authorTags} @author tags in the patch."
  if [[ $authorTags != 0 ]] ; then
    add_jira_table -1 @author \
      "The patch appears to contain $authorTags @author tags which the Hadoop" \
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
  local testReferences
  local patchIsDoc

  big_console_header "Checking there are new or changed tests in the patch."

  start_clock

  testReferences=$("${GREP}" -c -i -e '^+++.*/test' "${PATCH_DIR}/patch")
  echo "There appear to be ${testReferences} test files referenced in the patch."
  if [[ ${testReferences} == 0 ]] ; then
    if [[ ${JENKINS} == "true" ]] ; then
      # if component has documentation in it, we skip this part.
      # really need a better test here
      patchIsDoc=$("${GREP}" -c -i 'title="documentation' "${PATCH_DIR}/jira")
      if [[ ${patchIsDoc} != 0 ]] ; then
        echo "The patch appears to be a documentation patch that doesn't require tests."
        add_jira_table 0 "tests included" \
        "The patch appears to be a documentation patch that doesn't require tests."
        return 0
      fi
    fi

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

  big_console_header "Determining number of patched javadoc warnings"

  start_clock

  echo "${MVN} clean test javadoc:javadoc -DskipTests -Pdocs -D${PROJECT_NAME}PatchProcess > ${PATCH_DIR}/patchJavadocWarnings.txt 2>&1"
  if [[ -d hadoop-project ]]; then
    (cd hadoop-project; ${MVN} install > /dev/null 2>&1)
  fi
  if [[ -d hadoop-common-project/hadoop-annotations ]]; then
    (cd hadoop-common-project/hadoop-annotations; ${MVN} install > /dev/null 2>&1)
  fi
  ${MVN} clean test javadoc:javadoc -DskipTests -Pdocs -D${PROJECT_NAME}PatchProcess > "${PATCH_DIR}/patchJavadocWarnings.txt" 2>&1
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

  big_console_header "Determining number of patched javac warnings."

  start_clock

  echo "${MVN} clean test -DskipTests -D${PROJECT_NAME}PatchProcess ${NATIVE_PROFILE} -Ptest-patch > ${PATCH_DIR}/patchJavacWarnings.txt 2>&1"
  ${MVN} clean test -DskipTests -D${PROJECT_NAME}PatchProcess ${NATIVE_PROFILE} -Ptest-patch > "${PATCH_DIR}/patchJavacWarnings.txt" 2>&1
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
        "$((patchJavacWarnings-${PATCH_BRANCH}JavacWarnings))" \
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

  echo "${MVN} clean apache-rat:check -D${PROJECT_NAME}PatchProcess > ${PATCH_DIR}/patchReleaseAuditOutput.txt 2>&1"
  ${MVN} apache-rat:check -D${PROJECT_NAME}PatchProcess > "${PATCH_DIR}/patchReleaseAuditOutput.txt" 2>&1
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
  big_console_header "Installing all of the jars"

  start_clock
  echo "${MVN} install -Dmaven.javadoc.skip=true -DskipTests -D${PROJECT_NAME}PatchProcess  > ${PATCH_DIR}/jarinstall.txt 2>&1"
  ${MVN} install -Dmaven.javadoc.skip=true -DskipTests -D${PROJECT_NAME}PatchProcess > "${PATCH_DIR}/jarinstall.txt" 2>&1
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
  big_console_header "Determining number of patched Findbugs warnings."

  if [[ ! -e "${FINDBUGS_HOME}/bin/findbugs" ]]; then
    printf "\n\n%s is not executable.\n\n" "${FINDBUGS_HOME}/bin/findbugs"
    return 1
  fi

  start_clock

  local findbugs_version=$("${FINDBUGS_HOME}/bin/findbugs" -version)
  local modules=${CHANGED_MODULES}
  local rc=0
  local module_suffix
  local findbugsWarnings=0
  local relative_file
  local newFindbugsWarnings
  local findbugsWarnings

  for module in ${modules}
  do
    pushd "${module}" >/dev/null
    echo "  Running findbugs in ${module}"
    module_suffix=$(basename "${module}")
    echo "${MVN} clean test findbugs:findbugs -DskipTests -D${PROJECT_NAME}PatchProcess < /dev/null > ${PATCH_DIR}/patchFindBugsOutput${module_suffix}.txt 2>&1"
    ${MVN} clean test findbugs:findbugs -DskipTests -D${PROJECT_NAME}PatchProcess \
      < /dev/null \
      > "${PATCH_DIR}/patchFindBugsOutput${module_suffix}.txt" 2>&1
    (( rc = rc + $? ))
    popd >/dev/null
  done

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
      add_jira_footer "Findbugs warnings" "@@BASE@@/patchprocess/newPatchFindbugsWarnings${module_suffix}.html"
    fi
  done < <(find "${BASEDIR}" -name findbugsXml.xml)

  if [[ ${findbugsWarnings} -gt 0 ]] ; then
    add_jira_table -1 findbugs "The patch appears to introduce $findbugsWarnings new Findbugs (version ${findbugs_version}) warnings."
    return 1
  fi

  add_jira_table +1 findbugs "The patch does not introduce any new Findbugs (version ${findbugs_version}) warnings."
  return 0
}

## @description  Some crazy people like Eclipse.  Make sure Maven's eclipse generation
## @description  works so they don't freak out.
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function check_mvn_eclipse
{
  big_console_header "Running mvn eclipse:eclipse."

  start_clock

  echo "${MVN} eclipse:eclipse -D${PROJECT_NAME}PatchProcess > ${PATCH_DIR}/patchEclipseOutput.txt 2>&1"
  ${MVN} eclipse:eclipse -D${PROJECT_NAME}PatchProcess > "${PATCH_DIR}/patchEclipseOutput.txt" 2>&1
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
function populate_unit_table
{
  local reason=$1
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
  big_console_header "Running unit tests"

  start_clock

  local failed_tests=""
  local modules=${CHANGED_MODULES}
  local building_common=0
  local hdfs_modules
  local ordered_modules
  local failed_test_builds=""
  local test_timeouts=""
  local test_logfile
  local test_build_result
  local module_test_timeouts
  local result

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
      echo "  ${MVN} compile ${NATIVE_PROFILE} -D${PROJECT_NAME}PatchProcess"
      if ! ${MVN} compile ${NATIVE_PROFILE} -D${PROJECT_NAME}PatchProcess; then
        add_jira_table -1 "core tests" "Failed to build the native portion " \
          "of hadoop-common prior to running the unit tests in ${ordered_modules}"
        return 1
      fi
    fi
  fi

  result=0
  for module in ${ordered_modules}; do
    pushd "${module}" >/dev/null
    module_suffix=$(basename "${module}")
    test_logfile=${PATCH_DIR}/testrun_${module_suffix}.txt
    echo "  Running tests in ${module}"
    echo "  ${MVN} clean install -fae ${NATIVE_PROFILE} ${REQUIRE_TEST_LIB_HADOOP} -D${PROJECT_NAME}PatchProcess"
    ${MVN} clean install -fae ${NATIVE_PROFILE} ${REQUIRE_TEST_LIB_HADOOP} -D${PROJECT_NAME}PatchProcess > "${test_logfile}" 2>&1
    test_build_result=$?

    add_jira_footer "${module} test log" "@@BASE@@/testrun_${module_suffix}.txt"

    # shellcheck disable=2016
    module_test_timeouts=$(${AWK} '/^Running / { if (last) { print last } last=$2 } /^Tests run: / { last="" }' "${test_logfile}")
    if [[ -n "${module_test_timeouts}" ]] ; then
      test_timeouts="${test_timeouts} ${module_test_timeouts}"
      result=1
    fi
    #shellcheck disable=SC2026,SC2038
    module_failed_tests=$(find . -name 'TEST*.xml'\
      | xargs "${GREP}" -l -E "<failure|<error"\
      | ${AWK} -F/ '{sub("TEST-org.apache.",""); sub(".xml",""); print $NF}')
    if [[ -n "${module_failed_tests}" ]] ; then
      failed_tests="${failed_tests} ${module_failed_tests}"
      result=1
    fi
    if [[ ${test_build_result} != 0 && -z "${module_failed_tests}" && -z "${module_test_timeouts}" ]] ; then
      failed_test_builds="${failed_test_builds} ${module}"
      result=1
    fi
    popd >/dev/null
  done
  if [[ $result == 1 ]]; then
    add_jira_table -1 "core tests" "Tests failed in ${modules}."
    if [[ -n "${failed_tests}" ]] ; then
      # shellcheck disable=SC2086
      populate_unit_table "Failed unit tests" ${failed_tests}
    fi
    if [[ -n "${test_timeouts}" ]] ; then
      # shellcheck disable=SC2086
      populate_unit_table "Timed out tests" ${test_timeouts}
    fi
    if [[ -n "${failed_test_builds}" ]] ; then
      # shellcheck disable=SC2086
      populate_unit_table "Failed build" ${failed_test_builds}
    fi
  else
    add_jira_table +1 "core tests" "The patch passed unit tests in ${modules}."
  fi
  if [[ ${JENKINS} == true ]]; then
    add_jira_footer "Test Results" "${BUILD_URL}/testReport/"
  fi
  return ${result}
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
  local normaltop
  local line
  local seccoladj=$(findlargest 2 "${JIRA_COMMENT_TABLE[@]}")

  if [[ ${result} == 0 ]]; then
    printf "\n\n+1 overall\n\n"
  else
    printf "\n\n-1 overall\n\n"
  fi

  if [[ ${seccoladj} -lt 10 ]]; then
    seccoladj=10
  fi

  seccoladj=$((seccoladj + 2 ))
  i=0
  until [[ $i -eq ${#JIRA_HEADER[@]} ]]; do
    printf "%s\n" "${JIRA_HEADER[${i}]}"
    ((i=i+1))
  done

  printf "| %s | %*s |  %s  | %s\n" "Vote" ${seccoladj} Subsystem Runtime "Comment"

  i=0
  until [[ $i -eq ${#JIRA_COMMENT_TABLE[@]} ]]; do
    ourstring=$(echo "${JIRA_COMMENT_TABLE[${i}]}" | tr -s ' ')
    vote=$(echo "${ourstring}" | cut -f2 -d\|)
    vote=$(colorstripper "${vote}")
    subs=$(echo "${ourstring}"  | cut -f3 -d\|)
    ela=$(echo "${ourstring}" | cut -f4 -d\|)
    comment=$(echo "${ourstring}"  | cut -f5 -d\|)

    echo "${comment}" | fold -s -w $((78-seccoladj-21)) > "${PATCH_DIR}/comment.1"
    normaltop=$(head -1 "${PATCH_DIR}/comment.1")
    ${SED} -e '1d' "${PATCH_DIR}/comment.1"  > "${PATCH_DIR}/comment.2"

    printf "| %4s | %*s | %-7s |%-s\n" "${vote}" ${seccoladj} \
      "${subs}" "${ela}" "${normaltop}"
    while read line; do
      printf "|      | %*s |           | %-s\n" ${seccoladj} " " "${line}"
    done < "${PATCH_DIR}/comment.2"

    ((i=i+1))
    rm "${PATCH_DIR}/comment.2" "${PATCH_DIR}/comment.1" 2>/dev/null
  done

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

  printf "\n\n|| Subsystem || Report/Notes ||\n"
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

  if [[ ${JENKINS} != "true" ]] ; then
    return 0
  fi

  big_console_header "Adding comment to JIRA"

  add_jira_footer "Console output" "@@BASE@@/console"

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

  { echo "\\\\" ; echo "\\\\"; } >>  "${commentfile}"

  i=0
  until [[ $i -eq ${#JIRA_TEST_TABLE[@]} ]]; do
    printf "%s\n" "${JIRA_TEST_TABLE[${i}]}" >> "${commentfile}"
    ((i=i+1))
  done

  { echo "\\\\" ; echo "\\\\"; } >>  "${commentfile}"

  echo "|| Subsystem || Report/Notes ||" >> "${commentfile}"
  i=0
  until [[ $i -eq ${#JIRA_FOOTER_TABLE[@]} ]]; do
    comment=$(echo "${JIRA_FOOTER_TABLE[${i}]}" |
              ${SED} -e "s,@@BASE@@,${BUILD_URL}/artifact/patchprocess,g")
    printf "%s\n" "${comment}" >> "${commentfile}"
    ((i=i+1))
  done

  printf "\n\nThis message was automatically generated.\n\n" >> "${commentfile}"

  export USER=hudson
  ${JIRACLI} -s https://issues.apache.org/jira \
             -a addcomment -u hadoopqa \
             -p "${JIRA_PASSWD}" \
             --comment "$(cat ${commentfile})" \
             --issue "${ISSUE}"
  ${JIRACLI} -s https://issues.apache.org/jira \
             -a logout -u hadoopqa \
             -p "${JIRA_PASSWD}"
}

## @description  Clean the filesystem as appropriate and then exit
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        runresult
function cleanup_and_exit
{
  local result=$1

  if [[ ${JENKINS} == "true" ]] ; then
    if [[ -e "${PATCH_DIR}" ]] ; then
      hadoop_debug "mv ${PATCH_DIR} ${BASEDIR} "
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

    hadoop_debug "Running ${routine}"
    ${routine}

    (( RESULT = RESULT + $? ))
  done

  for plugin in ${PLUGINS}; do
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

  check_javac
  retval=$?
  if [[ ${retval} -gt 1 ]] ; then
    output_to_console 1
    output_to_jira 1
    cleanup_and_exit 1
  fi

  ((RESULT = RESULT + retval))

  for routine in check_javadoc check_apachelicense
  do

    hadoop_debug "Running ${routine}"
    $routine

    (( RESULT = RESULT + $? ))

  done

  for plugin in ${PLUGINS}; do
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
    hadoop_debug "Running ${routine}"
    ${routine}
    (( RESULT = RESULT + $? ))
  done

  for plugin in ${PLUGINS}; do
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

    check_unittests

    (( RESULT = RESULT + $? ))
  fi

  for plugin in ${PLUGINS}; do
    if declare -f ${plugin}_tests >/dev/null 2>&1; then
      hadoop_debug "Running ${plugin}_tests"
      #shellcheck disable=SC2086
      ${plugin}_tests
      (( RESULT = RESULT + $? ))
    fi
  done
}

## @description  Import content from test-patch.d
## @audience     private
## @stability    evolving
## @replaceable  no
function importplugins
{
  local i
  local files

  if [[ -d "${BINDIR}/test-patch.d" ]]; then
    files=(${BINDIR}/test-patch.d/*.sh)
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

git_checkout
RESULT=$?
if [[ ${JENKINS} == "true" ]] ; then
  if [[ ${RESULT} != 0 ]] ; then
    exit 100
  fi
fi

postcheckout

find_changed_modules

preapply

apply_patch_file

postapply

check_mvn_install

postinstall

runtests

output_to_console ${RESULT}
output_to_jira ${RESULT}
cleanup_and_exit ${RESULT}
