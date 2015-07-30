#!/usr/bin/env bash
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# Make sure that bash version meets the pre-requisite

if [[ -z "${BASH_VERSINFO}" ]] \
   || [[ "${BASH_VERSINFO[0]}" -lt 3 ]] \
   || [[ "${BASH_VERSINFO[0]}" -eq 3 && "${BASH_VERSINFO[1]}" -lt 2 ]]; then
  echo "bash v3.2+ is required. Sorry."
  exit 1
fi

RESULT=0

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
  if [[ -n "${YETUS_SHELL_SCRIPT_DEBUG}" ]]; then
    echo "[$(date) DEBUG]: $*" 1>&2
  fi
}

## @description  Clean the filesystem as appropriate and then exit
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        runresult
function cleanup_and_exit
{
  local result=$1

  if [[ ${PATCH_DIR} =~ ^/tmp/apply-patch
    && -d ${PATCH_DIR} ]]; then
    rm -rf "${PATCH_DIR}"
  fi

  # shellcheck disable=SC2086
  exit ${result}
}

## @description  Setup the default global variables
## @audience     public
## @stability    stable
## @replaceable  no
function setup_defaults
{
  PATCHURL=""
  OSTYPE=$(uname -s)

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

  DRYRUNMODE=false
  PATCH_DIR=/tmp
  while [[ -e ${PATCH_DIR} ]]; do
    PATCH_DIR=/tmp/apply-patch-${RANDOM}.${RANDOM}
  done
  PATCHMODES=("git" "patch")
  PATCHMODE=""
  PATCHPREFIX=0
}

## @description  Print the usage information
## @audience     public
## @stability    stable
## @replaceable  no
function yetus_usage
{
  echo "Usage: apply-patch.sh [options] patch-file | issue-number | http"
  echo
  echo "--debug                If set, then output some extra stuff to stderr"
  echo "--dry-run              Check for patch viability without applying"
  echo "--patch-dir=<dir>      The directory for working and output files (default '/tmp/apply-patch-(random))"
  echo
  echo "Shell binary overrides:"
  echo "--file-cmd=<cmd>       The 'file' command to use (default 'file')"
  echo "--grep-cmd=<cmd>       The 'grep' command to use (default 'grep')"
  echo "--git-cmd=<cmd>        The 'git' command to use (default 'git')"
  echo "--patch-cmd=<cmd>      The GNU-compatible 'patch' command to use (default 'patch')"
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
      --debug)
        YETUS_SHELL_SCRIPT_DEBUG=true
      ;;
      --dry-run)
        DRYRUNMODE=true
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
        yetus_usage
        exit 0
      ;;
      --patch-cmd=*)
        PATCH=${i#*=}
      ;;
      --patch-dir=*)
        PATCH_DIR=${i#*=}
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
        PATCH_OR_ISSUE=${i#*=}
      ;;
    esac
  done

  if [[ ! -d ${PATCH_DIR} ]]; then
    mkdir -p "${PATCH_DIR}"
    if [[ $? != 0 ]] ; then
      yetus_error "ERROR: Unable to create ${PATCH_DIR}"
      cleanup_and_exit 1
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

  # Allow passing "-" for stdin patches
  if [[ ${PATCH_OR_ISSUE} == - ]]; then
    PATCH_FILE="${PATCH_DIR}/patch"
    cat /dev/fd/0 > "${PATCH_FILE}"
  elif [[ -f ${PATCH_OR_ISSUE} ]]; then
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
          yetus_error "ERROR: Unable to fetch ${PATCH_OR_ISSUE}."
          cleanup_and_exit 1
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
        echo "${ISSUE} patch is being downloaded at $(date) from"
      fi
    fi
    if [[ -z "${PATCH_FILE}" ]]; then
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
    guess_patch_file "${PATCH_FILE}"
    if [[ $? != 0 ]]; then
      yetus_error "ERROR: ${PATCHURL} is not a patch file."
      cleanup_and_exit 1
    else
      yetus_debug "The patch ${PATCHURL} was not named properly, but it looks like a patch file. proceeding, but issue/branch matching might go awry."
    fi
  fi
}

## @description  if patch-level zero, then verify we aren't
## @description  just adding files
## @audience     public
## @stability    stable
## @param        filename
## @param        command
## @param        [..]
## @replaceable  no
## @returns      $?
function verify_zero
{
  local logfile=$1
  shift
  local dir

  # don't return /dev/null
  # shellcheck disable=SC2016
  changed_files1=$(${AWK} 'function p(s){if(s!~"^/dev/null"){print s}}
    /^diff --git /   { p($3); p($4) }
    /^(\+\+\+|---) / { p($2) }' "${PATCH_DIR}/patch" | sort -u)

  # maybe we interpreted the patch wrong? check the log file
  # shellcheck disable=SC2016
  changed_files2=$(${GREP} -E '^[cC]heck' "${logfile}" \
    | ${AWK} '{print $3}' \
    | ${SED} -e 's,\.\.\.$,,g')

  for filename in ${changed_files1} ${changed_files2}; do

    # leading prefix = bad
    if [[ ${filename} =~ ^(a|b)/ ]]; then
      return 1
    fi

    # touching an existing file is proof enough
    # that pl=0 is good
    if [[ -f ${filename} ]]; then
      return 0
    fi

    dir=$(dirname "${filename}" 2>/dev/null)
    if [[ -n ${dir} && -d ${dir} ]]; then
      return 0
    fi
  done

  # ¯\_(ツ)_/¯ - no way for us to know, all new files with no prefix!
  yetus_error "WARNING: Patch only adds files; using patch level ${PATCHPREFIX}"
  return 0
}

## @description  run the command, sending stdout and stderr to the given filename
## @audience     public
## @stability    stable
## @param        filename
## @param        command
## @param        [..]
## @replaceable  no
## @returns      $?
function run_and_redirect
{
  local logfile=$1
  shift

  # to the log
  echo "${*}" > "${logfile}"
  # the actual command
  "${@}" >> "${logfile}" 2>&1
}

## @description git patch dryrun
## @replaceable  no
## @audience     private
## @stability    evolving
function git_dryrun
{
  local prefixsize=${1:-0}

  while [[ ${prefixsize} -lt 4
    && -z ${PATCHMODE} ]]; do
    run_and_redirect "${PATCH_DIR}/apply-patch-git-dryrun.log" \
       "${GIT}" apply --binary -v --check "-p${prefixsize}" "${PATCH_FILE}"
    if [[ $? == 0 ]]; then
      PATCHPREFIX=${prefixsize}
      PATCHMODE=git
      echo "Verifying the patch:"
      cat "${PATCH_DIR}/apply-patch-git-dryrun.log"
      break
    fi
    ((prefixsize=prefixsize+1))
  done

  if [[ ${prefixsize} -eq 0 ]]; then
    verify_zero "${PATCH_DIR}/apply-patch-git-dryrun.log"
    if [[ $? != 0 ]]; then
      PATCHMODE=""
      PATCHPREFIX=""
      git_dryrun 1
    fi
  fi
}

## @description  patch patch dryrun
## @replaceable  no
## @audience     private
## @stability    evolving
function patch_dryrun
{
  local prefixsize=${1:-0}

  while [[ ${prefixsize} -lt 4
    && -z ${PATCHMODE} ]]; do
    run_and_redirect "${PATCH_DIR}/apply-patch-patch-dryrun.log" \
      "${PATCH}" "-p${prefixsize}" -E --dry-run < "${PATCH_FILE}"
    if [[ $? == 0 ]]; then
      PATCHPREFIX=${prefixsize}
      PATCHMODE=patch
      if [[ ${DRYRUNMODE} == true ]]; then
        echo "Verifying the patch:"
        cat "${PATCH_DIR}/apply-patch-patch-dryrun.log"
      fi
      break
    fi
    ((prefixsize=prefixsize+1))
  done

  if [[ ${prefixsize} -eq 0 ]]; then
    verify_zero "${PATCH_DIR}/apply-patch-patch-dryrun.log"
    if [[ $? != 0 ]]; then
      PATCHMODE=""
      PATCHPREFIX=""
      patch_dryrun 1
    fi
  fi
}

## @description  driver for dryrun methods
## @replaceable  no
## @audience     private
## @stability    evolving
function dryrun
{
  local method

  for method in "${PATCHMODES[@]}"; do
    if declare -f ${method}_dryrun >/dev/null; then
      "${method}_dryrun"
    fi
    if [[ -n ${PATCHMODE} ]]; then
      break
    fi
  done

  if [[ -n ${PATCHMODE} ]]; then
    RESULT=0
    return 0
  fi
  RESULT=1
  return 1
}

## @description  git patch apply
## @replaceable  no
## @audience     private
## @stability    evolving
function git_apply
{
  echo "Applying the patch:"
  run_and_redirect "${PATCH_DIR}/apply-patch-git-apply.log" \
    "${GIT}" apply --binary -v --stat --apply "-p${PATCHPREFIX}" "${PATCH_FILE}"
  ${GREP} -v "^Checking" "${PATCH_DIR}/apply-patch-git-apply.log"
}


## @description  patch patch apply
## @replaceable  no
## @audience     private
## @stability    evolving
function patch_apply
{
  echo "Applying the patch:"
  run_and_redirect "${PATCH_DIR}/apply-patch-patch-apply.log" \
    "${PATCH}" "-p${PATCHPREFIX}" -E < "${PATCH_FILE}"
  cat "${PATCH_DIR}/apply-patch-patch-apply.log"
}


## @description  driver for patch apply methods
## @replaceable  no
## @audience     private
## @stability    evolving
function apply
{
  if declare -f ${PATCHMODE}_apply >/dev/null; then
    "${PATCHMODE}_apply"
    if [[ $? -gt 0 ]]; then
      RESULT=1
    else
      RESULT=0
    fi
  else
    yetus_error "ERROR: Patching method ${PATCHMODE} does not have a way to apply patches!"
    RESULT=1
  fi
}

trap "cleanup_and_exit 1" HUP INT QUIT TERM

setup_defaults

parse_args "$@"

locate_patch

dryrun

if [[ ${RESULT} -gt 0 ]]; then
  yetus_error "ERROR: Aborting! The patch cannot be verified."
  cleanup_and_exit ${RESULT}
fi

if [[ ${DRYRUNMODE} == false ]]; then
  apply
fi

cleanup_and_exit ${RESULT}
