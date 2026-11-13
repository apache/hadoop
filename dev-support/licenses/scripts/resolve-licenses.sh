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

# Offline-first license text resolution.
#
# Reads the distinct licenses from a THIRD-PARTY.txt report and resolves
# each one against the versioned texts in dev-support/licenses/texts/.
# Nothing is downloaded: network access is only used by the optional
# aggregate-download-licenses goal (the optional network-download
# input of the standing license-validation workflow).
#
# The report lines look like:
#     (License One) (License Two) Artifact Name (g:a:v - url)
# Every leading parenthesized group is a license name — a dependency can
# carry several, and names may themselves contain parentheses (e.g.
# "(GNU General Public License (GPL), version 2, with the Classpath
# exception)") — while the last group is the g:a:v coordinate. A naive
# '\([^()]*\)' match loses both; this parser walks the groups instead.
#
# Usage: resolve-licenses.sh <THIRD-PARTY.txt> <texts-dir> <output-dir> [--strict]
#   --strict  exit 1 if any license has no local text, or a stored text is
#             HTML junk (an error page saved by a download, not a license)
set -euo pipefail

report="$1"
texts_dir="$2"
out_dir="$3"
strict="${4:-}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=license-slugs.sh
source "$script_dir/license-slugs.sh"

mkdir -p "$out_dir"
missing=()

# Distinct license names: the license groups are the consecutive run of
# parenthesized groups at the start of the line, each separated from the
# next by a single " ", e.g.
#     (BSD 2-Clause) (Public Domain, per CC0) HdrHistogram (g:a:v - url)
# Artifact names may carry their own parens ("GAX (...) for Java (Core)")
# but those never join the run, and the final group is the g:a:v
# coordinate. Names may themselves contain parentheses (the JMH
# GPL-with-Classpath-exception entry), so the walk tracks depth.
extract_license_names() {
  awk '
    {
      line = $0
      sub(/^[ \t]+/, "", line)
      n = length(line)
      depth = 0
      group = ""
      ngroups = 0
      i = 1
      while (i <= n) {
        c = substr(line, i, 1)
        if (depth == 0) {
          if (c == "(" && ngroups == 0) {
            group = ""
            depth = 1
            i++
            continue
          }
          break   # artifact name started; the run is complete
        }
        if (c == ")") {
          depth--
          if (depth == 0) {
            groups[++ngroups] = group
            # the run continues only when exactly " (" follows a close
            if (substr(line, i + 1, 2) == " (") {
              i += 3
              depth = 1
              group = ""
              continue
            }
            i++
            continue
          }
          group = group c
          i++
          continue
        }
        if (c == "(") { depth++ }
        group = group c
        i++
      }
      # every group of the leading run is a license name; the g:a:v
      # coordinate never joins the run (an artifact name always sits
      # between it and the licenses)
      for (g = 1; g <= ngroups; g++) print groups[g]
    }
  ' "$report"
}

while IFS= read -r license_name; do
  slug="$(license_slug "$license_name")"
  src="$texts_dir/LICENSE-$slug.txt"
  if [[ -f "$src" ]]; then
    if grep -qiE '<!doctype|<html' "$src"; then
      echo "REJECTED HTML junk: $src (license [$license_name]) needs re-seeding" >&2
      missing+=("$license_name (LICENSE-$slug.txt is HTML junk, re-seed it)")
      continue
    fi
    cp "$src" "$out_dir/"
  else
    missing+=("$license_name (LICENSE-$slug.txt)")
  fi
done < <(extract_license_names | sort -u)

resolved=$(find "$out_dir" -name 'LICENSE-*.txt' | wc -l)
echo "Resolved $resolved license text(s) from $texts_dir into $out_dir"

if [[ ${#missing[@]} -gt 0 ]]; then
  echo "MISSING local license texts:" >&2
  printf '  - %s\n' "${missing[@]}" >&2
  if [[ "$strict" == "--strict" ]]; then
    exit 1
  fi
fi
