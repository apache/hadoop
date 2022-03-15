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

# Usage: $0 [/path/to/some/example.jar;/path/to/another/example/created.jar]
#
# accepts a single command line argument with a colon separated list of
# paths to jars to check. Iterates through each such passed jar and checks
# all the contained paths to make sure they follow the below constructed
# safe list.

# We use +=, which is a bash 3.1+ feature
if [[ -z "${BASH_VERSINFO[0]}" ]] \
   || [[ "${BASH_VERSINFO[0]}" -lt 3 ]] \
   || [[ "${BASH_VERSINFO[0]}" -eq 3 && "${BASH_VERSINFO[1]}" -lt 1 ]]; then
  echo "bash v3.1+ is required. Sorry."
  exit 1
fi

set -e
set -o pipefail

# we have to allow the directories that lead to the org/apache/hadoop dir
allowed_expr="(^org/$|^org/apache/$"
# We allow the following things to exist in our client artifacts:
#   * classes in packages that start with org.apache.hadoop, which by
#     convention should be in a path that looks like org/apache/hadoop
allowed_expr+="|^org/apache/hadoop/"
#   * whatever in the "META-INF" directory
allowed_expr+="|^META-INF/"
#   * whatever under the "webapps" directory; for things shipped by yarn
allowed_expr+="|^webapps/"
#   * Resources files used by Hadoop YARN mini cluster
allowed_expr+="|^TERMINAL/"
#   * Hadoop's default configuration files, which have the form
#     "_module_-default.xml"
allowed_expr+="|^[^-]*-default.xml$"
#   * Hadoop's versioning properties files, which have the form
#     "_module_-version-info.properties"
allowed_expr+="|^[^-]*-version-info.properties$"
#   * Hadoop's application classloader properties file.
allowed_expr+="|^org.apache.hadoop.application-classloader.properties$"
#   * Used by JavaSandboxLinuxContainerRuntime as a default, loaded
#     from root, so can't relocate. :(
allowed_expr+="|^java.policy$"
#   * Used by javax.annotation
allowed_expr+="|^jndi.properties$"

allowed_expr+=")"
declare -i bad_artifacts=0
declare -a bad_contents
declare -a artifact_list
while IFS='' read -r -d ';' line; do artifact_list+=("$line"); done < <(printf '%s;' "$1")
if [ "${#artifact_list[@]}" -eq 0 ]; then
  echo "[ERROR] No artifacts passed in."
  exit 1
fi

jar_list_failed ()
{
    echo "[ERROR] Listing jar contents for file '${artifact}' failed."
    exit 1
}
trap jar_list_failed SIGUSR1

for artifact in "${artifact_list[@]}"; do
  # Note: On Windows the output from jar tf may contain \r\n's.  Normalize to \n.
  while IFS='' read -r line; do bad_contents+=("$line"); done < <( ( jar tf "${artifact}" | sed 's/\\r//' || kill -SIGUSR1 $$ ) | grep -v -E "${allowed_expr}" )
  if [ ${#bad_contents[@]} -gt 0 ]; then
    echo "[ERROR] Found artifact with unexpected contents: '${artifact}'"
    echo "    Please check the following and either correct the build or update"
    echo "    the allowed list with reasoning."
    echo ""
    for bad_line in "${bad_contents[@]}"; do
      echo "    ${bad_line}"
    done
    bad_artifacts=${bad_artifacts}+1
  else
    echo "[INFO] Artifact looks correct: '$(basename "${artifact}")'"
  fi
done

if [ "${bad_artifacts}" -gt 0 ]; then
  exit 1
fi
