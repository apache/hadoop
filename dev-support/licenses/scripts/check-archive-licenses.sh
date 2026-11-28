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
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied.  See the License for the specific
# language governing permissions and limitations under the License.

# Archive ground-truth gate: the assembled distribution archive's jars
# vs. its own LICENSE-binary and the collected dependency inventory.
#
# Compares the jars actually present in an assembled distribution archive
# (enumerated by enumerate-archive-jars.py) against:
#   - the archive's own LICENSE-binary (R38, compliance direction: any
#     shipped jar whose license is not stated by the archive's license
#     file is a failure),
#   - the graph inventory THIRD-PARTY.txt (R26/R27: jars that ship but the
#     Maven compile/runtime graph never saw; curated assembly includes are
#     the expected exception),
#   - the excludedArtifacts curation of the root pom (R39: a curated
#     exclusion that turns out to ship is a failure),
#   - the shaded-jars mapping (R37: graph GAs shaded into a top-level jar
#     are expected to be absent as standalone jars).
#
# Warns (does not fail) on:
#   - inventory entries with no standalone jar in the archive (the
#     shaded/curate worklist: R37 mapping or inventory pruning),
#   - version mismatches between LICENSE-binary and the shipped jars (R31).
#
# Usage: check-archive-licenses.sh <archive-jars.tsv> <LICENSE-binary>
#                                <THIRD-PARTY.txt> [repo-root]
#   <LICENSE-binary> must be the copy shipped INSIDE the archive, so the
#   gate checks the archive against what it actually distributes.
set -euo pipefail

tsv="$1"
manifest="$2"
report="$3"
root="${4:-$(cd "$(dirname "$0")/../../.." && pwd)}"
includes="$root/dev-support/licenses/curation/assembly-includes"
shaded="$root/dev-support/licenses/curation/shaded-jars"
mappings="$root/dev-support/licenses/curation/archive-jar-mappings"
promoted=""
manifest_gav=""
archive_gav=""

failures=0

# --- input validation -------------------------------------------------------
#
# A broken or mistyped input must fail, never pass vacuously: an archive
# with zero enumerated jars is a broken enumeration, not a compliant one
# (same principle as the parity gate's zero-entry manifest guard).
if [[ ! -f "$tsv" || ! -s "$tsv" ]]; then
  echo "ARCHIVE GATE FAILURE: no enumeration rows in '$tsv' — check the enumerate-archive-jars.py invocation" >&2
  exit 1
fi

# --- inputs ----------------------------------------------------------------

# R36 curated mappings: promote partial/unmapped rows (stem -> g:a) before
# any classification. stem = the manifest/filename artifactId, or the bare
# filename for rows with no metadata at all. A zero-byte mappings file
# would make the NR==FNR idiom consume the TSV as mappings — require -s.
if [[ -s "$mappings" ]]; then
  promoted="$(mktemp)"
  awk -F'\t' '
    NR==FNR {
      line = $0
      sub(/#.*/, "", line)
      split(line, kv, " ")
      if (kv[1] != "") map[tolower(kv[1])] = tolower(kv[2])
      next
    }
    {
      stem = $3
      if (stem == "") {
        n = split($1, seg, "/")
        base = seg[n]
        sub(/\.jar$/, "", base)
        stem = base
      }
      key = tolower(stem)
      if (key != "" && (key in map)) {
        split(map[key], mga, ":")
        if (mga[1] != "" && mga[2] != "") {
          $2 = mga[1]
          $3 = mga[2]
          $5 = "curated-mapping"
          $1 = $1
        }
      }
      print
    }' OFS='\t' "$mappings" "$tsv" > "$promoted"
  tsv="$promoted"
fi

# excludedArtifacts regex from the root pom (R39); tolerate a root with
# no pom (no exclusions configured) rather than dying. NOTE: `sed q` is
# unconditional and would quit after line 1 — extract with awk instead.
if [[ -f "$root/pom.xml" ]]; then
  exclude_pattern="$(awk '/<excludedArtifacts>/ { sub(/.*<excludedArtifacts>/, ""); sub(/<\/excludedArtifacts>.*/, ""); print; exit }' "$root/pom.xml")"
else
  exclude_pattern=""
fi
# the pom is XML: decode the entities as Maven does before handing the
# pattern to the plugin (the lookbehind's < is written &lt;)
exclude_pattern="${exclude_pattern//&lt;/<}"
exclude_pattern="${exclude_pattern//&gt;/>}"
exclude_pattern="${exclude_pattern//&amp;/"&"}"

# R47: the pom pattern must be anchored. The plugin matches "g:a" against
# [^:]+:[^:]*(PATTERN)[^:]*, so a bare alternative (junit-.*) is a substring
# match that also excludes my-junit-extras; only the (?<=:)(?:alt|...)$ form
# matches whole artifactIds. Bash ERE has no lookbehind, so the gate checks
# that form, rejects duplicate alternatives, and matches the alternatives
# itself as ^(alt|...)$ — the same set the plugin excludes.
exclude_ere=""
if [[ -n "$exclude_pattern" ]]; then
  anchor_prefix='(?<=:)(?:'
  anchor_suffix=')$'
  if [[ "$exclude_pattern" != "$anchor_prefix"*"$anchor_suffix" ]]; then
    echo "R47 FAILURE: excludedArtifacts '$exclude_pattern' is not anchored — write it as (?<=:)(?:alt|alt...)\$ (the plugin wraps it in [^:]*(...)[^:]*, so a bare alternative matches substrings)" >&2
    failures=1
  else
    inner="${exclude_pattern#"$anchor_prefix"}"
    inner="${inner%"$anchor_suffix"}"
    # top-level alternatives (a | inside parentheses does not split)
    duplicates="$(awk -v s="$inner" 'BEGIN {
      depth = 0; alt = ""
      for (i = 1; i <= length(s); i++) {
        c = substr(s, i, 1)
        if (c == "(") depth++
        else if (c == ")") depth--
        if (c == "|" && depth == 0) { print alt; alt = "" } else alt = alt c
      }
      print alt
    }' | sort | uniq -d)"
    if [[ -n "$duplicates" ]]; then
      echo "R47 FAILURE: excludedArtifacts lists duplicate alternatives: $(tr '\n' ' ' <<<"$duplicates")" >&2
      failures=1
    fi
    exclude_ere="^(${inner})\$"
  fi
fi

# g:a pairs from the archive enumeration, third-party only (own
# org.apache.hadoop* jars are covered by the archive's top-level LICENSE)
archive_all="$(mktemp)"
archive_gas="$(mktemp)"
own="$(mktemp)"
partial="$(mktemp)"
unmapped="$(mktemp)"
# awk only creates its redirect targets when a row matches; pre-create so
# later reads do not depend on the archive content
touch "$own" "$partial" "$unmapped"
awk -F'\t' '
  {
    ga = tolower($2 ":" $3)
    if ($2 == "") {
      if ($3 == "") print $1 > "'"$unmapped"'"
      else print $0 > "'"$partial"'"
    } else if ($2 ~ /^org\.apache\.hadoop/) {
      print ga > "'"$own"'"
    } else {
      print ga
    }
  }' "$tsv" | LC_ALL=C sort -u > "$archive_gas"
cp "$archive_gas" "$archive_all"
LC_ALL=C sort -u "$own" -o "$own"
LC_ALL=C sort "$partial" -o "$partial" 2>/dev/null || true
LC_ALL=C sort "$unmapped" -o "$unmapped" 2>/dev/null || true

# g:a pairs from the archive's LICENSE-binary (same tolerant parsing as
# the parity gate)
manifest_gas="$(mktemp)"
if [[ -n "$manifest" && -f "$manifest" ]]; then
  # an empty manifest must not kill the gate (pipefail): no matches is a
  # valid — failing-later — state, every shipped jar is then unlicensed.
  # g:a comparison is case-insensitive (HikariCP/Objenesis jars carry
  # mixed-case artifactIds)
  grep -E '^[a-z][a-zA-Z0-9._-]*(:[a-zA-Z0-9._=-]+)+' "$manifest" \
    | sed 's/\.jar$//' | LC_ALL=C awk -F: '{print tolower($1 ":" $2)}' \
    | LC_ALL=C sort -u > "$manifest_gas" || true
fi

# g:a pairs from the graph inventory: the LAST parenthesized group of each
# report line is the g:a:v coordinate (names may carry their own parens)
inventory_gas="$(mktemp)"
LC_ALL=C awk '
  {
    line = $0
    n = length(line)
    depth = 0
    group = ""
    last = ""
    i = 1
    while (i <= n) {
      c = substr(line, i, 1)
      if (depth == 0) {
        if (c == "(") { depth = 1; group = ""; i++; continue }
        i++
        continue
      }
      if (c == ")") {
        depth--
        if (depth == 0) { last = group; group = "" }
        else { group = group c }
        i++
        continue
      }
      if (c == "(") depth++
      group = group c
      i++
    }
    split(last, parts, ":")
    if (parts[1] != "" && parts[2] != "") print tolower(parts[1] ":" parts[2])
  }
' "$report" | LC_ALL=C sort -u > "$inventory_gas"

includes_gas="$(mktemp)"
[[ -f "$includes" ]] && grep -vE '^[[:space:]]*(#|$)' "$includes" \
  | awk '{print tolower($1)}' | LC_ALL=C sort -u > "$includes_gas" || true

cleanup() { rm -f "$archive_all" "$archive_gas" "$own" "$partial" "$unmapped" "$manifest_gas" "$inventory_gas" "$includes_gas" "$manifest_gav" "$archive_gav"; if [[ -n "$promoted" ]]; then rm -f "$promoted"; fi; }
trap cleanup EXIT

echo "archive: $(wc -l < "$archive_gas") third-party g:a, $(wc -l < "$own") own org.apache.hadoop jars, $(wc -l < "$partial") partial, $(wc -l < "$unmapped") unmapped; manifest: $(wc -l < "$manifest_gas") g:a; inventory: $(wc -l < "$inventory_gas") g:a"

# --- R39: curated exclusions that ship -------------------------------------

if [[ -n "$exclude_ere" ]]; then
  # mapped third-party jars
  while IFS= read -r ga; do
    artifact="${ga#*:}"
    if [[ "$artifact" =~ $exclude_ere ]]; then
      echo "R39 FAILURE: curated exclusion ships in the archive: $ga (matches excludedArtifacts '$exclude_pattern' — remove the exclusion or the jar)" >&2
      failures=1
    fi
  done < "$archive_gas"
  # partial rows (manifest/filename-only): artifact is field 3 of the tsv.
  # Do NOT `read` the raw row: tab is IFS whitespace, so the empty group
  # field would collapse and shift artifactId into the wrong variable —
  # extract the fields with awk first
  while IFS=$'\t' read -r path artifact; do
    if [[ "$artifact" =~ $exclude_ere ]]; then
      echo "R39 FAILURE: curated exclusion ships in the archive: $path (artifact '$artifact' matches excludedArtifacts '$exclude_pattern')" >&2
      failures=1
    fi
  done < <(awk -F'\t' '$3 != "" {print $1 "\t" $3}' "$partial")
fi

# --- R36: jars that resist normalization ----------------------------------

if [[ -s "$unmapped" ]]; then
  echo "R36 FAILURE: jars in the archive that resist g:a normalization (add a curated mapping):" >&2
  cat "$unmapped" >&2
  failures=1
fi
if [[ -s "$partial" ]]; then
  echo "R36 FAILURE: jars with no group metadata (manifest/filename only — add a curated mapping):" >&2
  awk -F'\t' '{print "  " $1 "  (artifact " $3 ", version " $4 ", via " $5 ")"}' "$partial" >&2
  failures=1
fi

# --- R38: archive jars the archive's license file does not cover ----------

unlicensed="$(LC_ALL=C comm -23 "$archive_gas" "$manifest_gas")"
if [[ -n "$unlicensed" ]]; then
  echo "R38 FAILURE: jars shipped in the archive whose license its LICENSE-binary does not state:" >&2
  printf '%s\n' "$unlicensed" >&2
  failures=1
fi

# --- R26/R27: archive jars the graph inventory never saw -------------------

off_graph="$(LC_ALL=C comm -23 "$archive_gas" "$inventory_gas")"
if [[ -n "$off_graph" ]]; then
  expected="$(printf '%s\n' "$off_graph" | LC_ALL=C comm -12 - "$includes_gas")"
  unexpected="$(printf '%s\n' "$off_graph" | LC_ALL=C comm -23 - "$includes_gas")"
  if [[ -n "$expected" ]]; then
    echo "expected: assembly-included jars off the compile/runtime graph (assembly-includes):" >&2
    printf '%s\n' "$expected" >&2
  fi
  if [[ -n "$unexpected" ]]; then
    echo "R26 FAILURE: jars in the archive absent from the graph inventory and not curated as assembly includes:" >&2
    printf '%s\n' "$unexpected" >&2
    failures=1
  fi
fi

# --- graph claims without a standalone archive jar (worklist) --------------

graph_only="$(LC_ALL=C comm -13 "$archive_all" "$inventory_gas")"
if [[ -n "$graph_only" ]]; then
  echo "WORKLIST: inventory entries with no standalone jar in the archive" >&2
  echo "(expected for shaded/embedded coordinates — record them in dev-support/licenses/curation/shaded-jars;)" >&2
  echo "(otherwise curate the inventory or the exclusion):" >&2
  while IFS= read -r ga; do
    mapping=""
    if [[ -f "$shaded" ]]; then
      mapping="$(grep -E "^[[:space:]]*${ga//./\\.}[[:space:]]" "$shaded" | head -1 || true)"
    fi
    if [[ -n "$mapping" ]]; then
      echo "  $ga  [mapped: $mapping]" >&2
    else
      echo "  $ga  [no mapping — curate]" >&2
    fi
  done <<< "$graph_only"
fi

# --- R31: version consistency (warning) -------------------------------------

if [[ -n "$manifest" && -f "$manifest" ]]; then
  manifest_gav="$(mktemp)"
  archive_gav="$(mktemp)"
  cleanup() { rm -f "$archive_all" "$archive_gas" "$own" "$partial" "$unmapped" "$manifest_gas" "$inventory_gas" "$includes_gas" "$manifest_gav" "$archive_gav"; if [[ -n "$promoted" ]]; then rm -f "$promoted"; fi; }
  # g:a:v the archive's LICENSE-binary lists (case-insensitive: versions
  # like 4.1.137.Final arrive as 4.1.137.final from the enumeration)
  grep -E '^[a-z][a-zA-Z0-9._-]*(:[a-zA-Z0-9._=-]+)+' "$manifest" \
    | sed 's/\.jar$//' | awk -F: '{print tolower($1 ":" $2 ":" $3)}' \
    | LC_ALL=C sort -u > "$manifest_gav" || true
  # g:a:v of the third-party jars actually shipped (skip versionless rows:
  # promoted/manifest rows may carry none, and g:a: noise helps nobody)
  awk -F'\t' '$2 != "" && $4 != "" && $2 !~ /^org\.apache\.hadoop/ {print tolower($2 ":" $3 ":" $4)}' "$tsv" \
    | LC_ALL=C sort -u > "$archive_gav"
  mismatch="$(LC_ALL=C comm -13 "$manifest_gav" "$archive_gav")"
  if [[ -n "$mismatch" ]]; then
    echo "R31 WARNING: shipped jar versions absent from the archive's LICENSE-binary:" >&2
    printf '%s\n' "$mismatch" >&2
  fi
fi

if [[ "$failures" -ne 0 ]]; then
  echo "Archive gate FAILED." >&2
  exit 1
fi
echo "Archive gate passed: every shipped jar's license is stated and no curated exclusion ships."
