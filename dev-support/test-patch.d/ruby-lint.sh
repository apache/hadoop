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

add_plugin ruby_lint

RUBY_LINT_TIMER=0

RUBY_LINT=${RUBY_LINT:-$(which ruby-lint 2>/dev/null)}

function ruby_lint_usage
{
  echo "Ruby-lint specific:"
  echo "--ruby-lint=<path> path to ruby-lint executable"
}

function ruby_lint_parse_args
{
  local i

  for i in "$@"; do
    case ${i} in
    --ruby-lint=*)
      RUBY_LINT=${i#*=}
    ;;
    esac
  done
}

function ruby_lint_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.rb$ ]]; then
    add_test ruby_lint
  fi
}

function ruby_lint_preapply
{
  local i

  verify_needed_test ruby_lint
  if [[ $? == 0 ]]; then
    return 0
  fi

  big_console_header "ruby-lint plugin: prepatch"

  if [[ ! -x ${RUBY_LINT} ]]; then
    yetus_error "${RUBY_LINT} does not exist."
    return 0
  fi

  start_clock

  echo "Running ruby-lint against modified ruby scripts."
  pushd "${BASEDIR}" >/dev/null
  for i in ${CHANGED_FILES}; do
    if [[ ${i} =~ \.rb$ && -f ${i} ]]; then
      ${RUBY_LINT} -p syntastic "${i}" | sort -t : -k 1,1 -k 3,3n -k 4,4n >> "${PATCH_DIR}/branch-ruby-lint-result.txt"
    fi
  done
  popd >/dev/null
  # keep track of how much as elapsed for us already
  RUBY_LINT_TIMER=$(stop_clock)
  return 0
}

function ruby_lint_postapply
{
  local i
  local numPrepatch
  local numPostpatch
  local diffPostpatch

  verify_needed_test ruby_lint
  if [[ $? == 0 ]]; then
    return 0
  fi

  big_console_header "ruby-lint plugin: postpatch"

  if [[ ! -x ${RUBY_LINT} ]]; then
    yetus_error "${RUBY_LINT} is not available."
    add_vote_table 0 ruby-lint "Ruby-lint was not available."
    return 0
  fi

  start_clock

  # add our previous elapsed to our new timer
  # by setting the clock back
  offset_clock "${RUBY_LINT_TIMER}"

  echo "Running ruby-lint against modified ruby scripts."
  # we re-check this in case one has been added
  pushd "${BASEDIR}" >/dev/null
  for i in ${CHANGED_FILES}; do
    if [[ ${i} =~ \.rb$ && -f ${i} ]]; then
      ${RUBY_LINT} -p syntastic "${i}" | sort -t : -k 1,1 -k 3,3n -k 4,4n >> "${PATCH_DIR}/patch-ruby-lint-result.txt"
    fi
  done
  popd >/dev/null

  # shellcheck disable=SC2016
  RUBY_LINT_VERSION=$(${RUBY_LINT} -v | ${AWK} '{print $2}')
  add_footer_table ruby-lint "${RUBY_LINT_VERSION}"

  calcdiffs "${PATCH_DIR}/branch-ruby-lint-result.txt" "${PATCH_DIR}/patch-ruby-lint-result.txt" > "${PATCH_DIR}/diff-patch-ruby-lint.txt"
  diffPostpatch=$(${AWK} -F: 'BEGIN {sum=0} 4<NF {sum+=1} END {print sum}' "${PATCH_DIR}/diff-patch-ruby-lint.txt")

  if [[ ${diffPostpatch} -gt 0 ]] ; then
    # shellcheck disable=SC2016
    numPrepatch=$(${AWK} -F: 'BEGIN {sum=0} 4<NF {sum+=1} END {print sum}' "${PATCH_DIR}/branch-ruby-lint-result.txt")

    # shellcheck disable=SC2016
    numPostpatch=$(${AWK} -F: 'BEGIN {sum=0} 4<NF {sum+=1} END {print sum}' "${PATCH_DIR}/patch-ruby-lint-result.txt")

    add_vote_table -1 ruby-lint "The applied patch generated "\
      "${diffPostpatch} new ruby-lint issues (total was ${numPrepatch}, now ${numPostpatch})."
    add_footer_table ruby-lint "@@BASE@@/diff-patch-ruby-lint.txt"
    return 1
  fi

  add_vote_table +1 ruby-lint "There were no new ruby-lint issues."
  return 0
}

function ruby_lint_postcompile
{
  declare repostatus=$1

  if [[ "${repostatus}" = branch ]]; then
    ruby_lint_preapply
  else
    ruby_lint_postapply
  fi
}
