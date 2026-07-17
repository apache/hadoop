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

# Usage: check-coverage-modules.sh <repo-root> <coverage-pom>
#
# Guards the hadoop-coverage aggregate module against silent drift: every
# "test-bearing" Maven module (jar packaging with a src/test/java directory)
# must either be listed as a dependency in hadoop-coverage/pom.xml or be named
# in dev-support/coverage-modules-allowlist.txt. If a test-bearing module is in
# neither, its coverage would be silently dropped from the aggregate report, so
# the build fails with a message telling the developer which module to add and
# where.
#
# The check inspects the source tree (not the Maven reactor), so it behaves the
# same under a full-reactor build and a partial `-pl` build.
#
# Scope and limitations (intentional, to keep the check simple):
#   - Modules are matched by artifactId only, not by groupId or version. The
#     check ensures a module is *present* in the aggregate; it does not validate
#     that its coordinates resolve. A wrong groupId/version is a Maven-resolution
#     error, surfaced by the build itself, not by this guard.
#   - "Covered" artifactIds are read from the coverage pom's top-level
#     <dependencies>. The coverage pom deliberately has no <dependencyManagement>
#     or plugin-level <dependencies>; if that changes, revisit the awk below.
#   - <artifactId>/<packaging>/<parent> tags are assumed to sit on a single line
#     each (the Hadoop pom convention); a multi-line element would be misparsed.

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <repo-root> <coverage-pom>" >&2
  exit 2
fi

repo_root="$1"
coverage_pom="$2"
allowlist="${repo_root}/dev-support/coverage-modules-allowlist.txt"

if [[ ! -f "${coverage_pom}" ]]; then
  echo "check-coverage-modules: coverage pom not found: ${coverage_pom}" >&2
  exit 2
fi

# Print a module's own artifactId: the first <artifactId> after </parent>.
# Hadoop poms always declare <parent> before the module's own coordinates, and
# <dependencies> come afterwards, so the first match is the module itself.
module_artifact_id() {
  awk '
    /<\/parent>/ { seen_parent = 1; next }
    seen_parent && match($0, /<artifactId>[^<]+<\/artifactId>/) {
      s = substr($0, RSTART, RLENGTH)
      gsub(/<\/?artifactId>/, "", s)
      gsub(/[[:space:]]/, "", s)
      print s
      exit
    }
  ' "$1"
}

# Print a module's packaging (defaults to "jar" when absent). A <parent> block
# never carries a <packaging>, so the first match is the module's own.
module_packaging() {
  local pkg
  pkg="$(grep -oE '<packaging>[^<]+</packaging>' "$1" | head -1 \
         | sed -E 's#</?packaging>##g' | tr -d '[:space:]' || true)"
  echo "${pkg:-jar}"
}

# Test-bearing modules: jar packaging AND a src/test/java directory.
test_bearing_file="$(mktemp)"
covered_file="$(mktemp)"
allowed_file="$(mktemp)"
trap 'rm -f "${test_bearing_file}" "${covered_file}" "${allowed_file}"' EXIT

while IFS= read -r pom; do
  dir="$(dirname "${pom}")"
  [[ -d "${dir}/src/test/java" ]] || continue
  [[ "$(module_packaging "${pom}")" == "jar" ]] || continue
  aid="$(module_artifact_id "${pom}")"
  [[ -n "${aid}" ]] && echo "${aid}"
done < <(find "${repo_root}" -name pom.xml -not -path '*/target/*') \
  | sort -u > "${test_bearing_file}"

# Sanity check: finding zero test-bearing modules means the tree was not scanned
# (wrong repo-root, relocated script). Fail loudly rather than pass vacuously.
if [[ ! -s "${test_bearing_file}" ]]; then
  echo "check-coverage-modules: no test-bearing modules found under" \
       "'${repo_root}' -- wrong repo root?" >&2
  exit 2
fi

# Covered modules: org.apache.hadoop artifactIds inside the coverage pom's
# top-level <dependencies> block.
awk '/<dependencies>/{d=1} /<\/dependencies>/{d=0} d' "${coverage_pom}" \
  | grep -oE '<artifactId>[^<]+</artifactId>' \
  | sed -E 's#</?artifactId>##g; s/[[:space:]]//g' \
  | sort -u > "${covered_file}"

# Allowlist: intentionally-excluded test-bearing modules (ignore blanks/comments).
if [[ -f "${allowlist}" ]]; then
  grep -vE '^[[:space:]]*(#|$)' "${allowlist}" | sed 's/[[:space:]]//g' \
    | sort -u > "${allowed_file}"
else
  : > "${allowed_file}"
fi

# Missing = test-bearing modules that are neither covered nor allowlisted.
missing="$(comm -23 "${test_bearing_file}" \
             <(sort -u "${covered_file}" "${allowed_file}"))"

# Non-fatal hygiene warning: allowlist entries that are no longer test-bearing.
stale="$(comm -13 "${test_bearing_file}" "${allowed_file}")"
if [[ -n "${stale}" ]]; then
  echo "check-coverage-modules: WARNING: stale allowlist entries (no longer a" \
       "test-bearing jar module); consider removing from" \
       "dev-support/coverage-modules-allowlist.txt:" >&2
  while IFS= read -r module; do echo "  - ${module}" >&2; done <<< "${stale}"
fi

if [[ -n "${missing}" ]]; then
  echo "" >&2
  echo "check-coverage-modules: FAILED" >&2
  echo "The following test-bearing modules are missing from the coverage" \
       "aggregate:" >&2
  while IFS= read -r module; do echo "  - ${module}" >&2; done <<< "${missing}"
  echo "" >&2
  echo "Add each as a <dependency> in hadoop-coverage/pom.xml so its coverage" >&2
  echo "is aggregated, or, if it is intentionally excluded, add it to" >&2
  echo "dev-support/coverage-modules-allowlist.txt (with a reason)." >&2
  exit 1
fi

count="$(wc -l < "${test_bearing_file}" | tr -d ' ')"
echo "check-coverage-modules: OK (${count} test-bearing modules accounted for)"
