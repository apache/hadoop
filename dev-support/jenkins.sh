#!/usr/bin/env bash

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

is_cpp_extension() {
  local cpp_extensions=("cc" "cpp" "h" "hpp")
  local seeking=$1

  for element in "${cpp_extensions[@]}"; do
    if [[ $element == "$seeking" ]]; then
      return 0
    fi
  done
  return 1
}

is_platform_change() {
  declare in_path
  in_path="${SOURCEDIR}"/"${1}"

  for path in "${SOURCEDIR}"/dev-support/docker/Dockerfile* "${SOURCEDIR}"/dev-support/docker/pkg-resolver/*.json; do
    if [ "${in_path}" == "${path}" ]; then
      echo "Found C++ platform related changes in ${in_path}"
      return 0
    fi
  done
  return 1
}

is_cpp_change() {
  shopt -s nocasematch

  local path=$1
  declare filename
  filename=$(basename -- "${path}")
  extension=${filename##*.}

  if is_cpp_extension "${extension}"; then
    echo "Found C++ changes in ${path}"
    return 0
  fi

  if [[ $filename =~ CMakeLists\.txt ]]; then
    echo "Found C++ build related changes in ${path}"
    return 0
  fi
  return 1
}

function check_ci_run() {
  firstCommitOfThisPr=$(git --git-dir "${SOURCEDIR}/.git" rev-parse origin/trunk)
  for path in $(git --git-dir "${SOURCEDIR}/.git" diff --name-only "${firstCommitOfThisPr}" HEAD); do
    if is_cpp_change "${path}"; then
      return 0
    fi

    if is_platform_change "${path}"; then
      return 0
    fi
  done

  if [ "$IS_OPTIONAL" -eq 0 ]; then
    return 0
  fi
  return 1
}

function run_ci() {
  TESTPATCHBIN="${WORKSPACE}/${YETUS}/precommit/src/main/shell/test-patch.sh"

  # this must be clean for every run
  if [[ -d "${WORKSPACE}/${PATCHDIR}" ]]; then
    rm -rf "${WORKSPACE:?}/${PATCHDIR}"
  fi
  mkdir -p "${WORKSPACE}/${PATCHDIR}"

  # if given a JIRA issue, process it. If CHANGE_URL is set
  # (e.g., Github Branch Source plugin), process it.
  # otherwise exit, because we don't want Hadoop to do a
  # full build.  We wouldn't normally do this check for smaller
  # projects. :)
  if [[ -n "${JIRA_ISSUE_KEY}" ]]; then
    YETUS_ARGS+=("${JIRA_ISSUE_KEY}")
  elif [[ -z "${CHANGE_URL}" ]]; then
    echo "Full build skipped" >"${WORKSPACE}/${PATCHDIR}/report.html"
    exit 0
  fi

  YETUS_ARGS+=("--patch-dir=${WORKSPACE}/${PATCHDIR}")

  # where the source is located
  YETUS_ARGS+=("--basedir=${WORKSPACE}/${SOURCEDIR}")

  # our project defaults come from a personality file
  YETUS_ARGS+=("--project=hadoop")
  YETUS_ARGS+=("--personality=${WORKSPACE}/${SOURCEDIR}/dev-support/bin/hadoop.sh")

  # lots of different output formats
  YETUS_ARGS+=("--brief-report-file=${WORKSPACE}/${PATCHDIR}/brief.txt")
  YETUS_ARGS+=("--console-report-file=${WORKSPACE}/${PATCHDIR}/console.txt")
  YETUS_ARGS+=("--html-report-file=${WORKSPACE}/${PATCHDIR}/report.html")

  # enable writing back to Github
  YETUS_ARGS+=(--github-token="${GITHUB_TOKEN}")

  # enable writing back to ASF JIRA
  YETUS_ARGS+=(--jira-password="${JIRA_PASSWORD}")
  YETUS_ARGS+=(--jira-user="${JIRA_USER}")

  # auto-kill any surefire stragglers during unit test runs
  YETUS_ARGS+=("--reapermode=kill")

  # set relatively high limits for ASF machines
  # changing these to higher values may cause problems
  # with other jobs on systemd-enabled machines
  YETUS_ARGS+=("--proclimit=5500")
  YETUS_ARGS+=("--dockermemlimit=22g")

  # -1 spotbugs issues that show up prior to the patch being applied
  YETUS_ARGS+=("--spotbugs-strict-precheck")

  # rsync these files back into the archive dir
  YETUS_ARGS+=("--archive-list=checkstyle-errors.xml,spotbugsXml.xml")

  # URL for user-side presentation in reports and such to our artifacts
  # (needs to match the archive bits below)
  YETUS_ARGS+=("--build-url-artifacts=artifact/out")

  # plugins to enable
  YETUS_ARGS+=("--plugins=all")

  # don't let these tests cause -1s because we aren't really paying that
  # much attention to them
  YETUS_ARGS+=("--tests-filter=checkstyle")

  # run in docker mode and specifically point to our
  # Dockerfile since we don't want to use the auto-pulled version.
  YETUS_ARGS+=("--docker")
  YETUS_ARGS+=("--dockerfile=${DOCKERFILE}")
  YETUS_ARGS+=("--mvn-custom-repos")

  # effectively treat dev-suport as a custom maven module
  YETUS_ARGS+=("--skip-dirs=dev-support")

  # help keep the ASF boxes clean
  YETUS_ARGS+=("--sentinel")

  # test with Java 8 and 11
  YETUS_ARGS+=("--java-home=/usr/lib/jvm/java-8-openjdk-amd64")
  YETUS_ARGS+=("--multijdkdirs=/usr/lib/jvm/java-11-openjdk-amd64")
  YETUS_ARGS+=("--multijdktests=compile")

  # custom javadoc goals
  YETUS_ARGS+=("--mvn-javadoc-goals=process-sources,javadoc:javadoc-no-fork")

  # write Yetus report as GitHub comment (YETUS-1102)
  YETUS_ARGS+=("--github-write-comment")
  YETUS_ARGS+=("--github-use-emoji-vote")

  "${TESTPATCHBIN}" "${YETUS_ARGS[@]}"
}

if check_ci_run; then
  run_ci
fi
