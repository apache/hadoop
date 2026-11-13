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

# Release-time license driver: ONE callable, fail-closed
# step that runs the full license generate-and-verify suite against the
# exact tarballs being voted on — the same checks the standing license
# validation runs, executed at release time: in the RC build (the
# create-release hook shape) and, with the same inputs, as the offline
# voter verifier.
#
# Release mode (default) — against built tarballs + a repo checkout:
#   run-release-checks.sh --tarballs <dir-or-file>... [--out <dir>]
#                         [--repo <root>]
#                         [--third-party <THIRD-PARTY.txt> | --collect]
#   Enumerates every binary tarball, resolves each shipped jar, generates
#   LICENSE-binary and licenses-binary/ from the enumerations, and runs
#   every gate fail-closed:
#     archive completeness | inventory-vs-archive | nameability |
#     curated exclusions | non-jar assets | NOTICE coverage |
#     shipped texts | committed drift
#   GREEN: publishes the RC artifact set (THIRD-PARTY.txt, generated
#   LICENSE-binary, licenses-binary/, sha512 files) plus an RM offline
#   checklist, into --out.
#   RED: publishes a fix list — every failed item with the exact entry
#   (in the per-check log) and the small configuration file where the
#   change belongs. Never exit 0 with a failed check; never edit the
#   license files in place.
#
# Voter mode — one-command verification of a staged or downloaded RC:
#   run-release-checks.sh --verify-only <rc-dir> [--repo <root>] [--keys <KEYS>]
#   <rc-dir> holds the release tarballs, their .sha512 files, and the
#   THIRD-PARTY.txt the RC carries (the proposal's two extra artifacts).
#   Verifies checksums (and GPG signatures when --keys is given),
#   re-enumerates each tarball, and re-runs the full gate against the
#   tarball's OWN shipped LICENSE-binary and the RC's THIRD-PARTY.txt,
#   including the drift check (shipped manifest == manifest regenerated
#   from the archive). Needs a checkout of the release tag (--repo) for
#   the curation files and the texts store. RED here is diagnostic: a
#   staged RC that fails must be replaced by a new RC, not edited.
#
# The pruning reports (unused-jar candidates, archive delta) are
# separate informational tools, deliberately not part of this step:
# they are conservative evidence and never gate a release.
#
# Exit codes: 0 all checks green; 1 one or more checks failed (fix list
# printed); 2 usage/environment error (nothing verified).

set -u -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROG="$(basename "$0")"

MODE=""
OUT=""
REPO=""
TARBALLS=()
THIRD_PARTY=""
COLLECT=0
VERIFY_DIR=""
KEYS=""

usage() { sed -n '20,46p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tarballs)
      # variadic: every following value until the next --flag is a
      # tarball source (dir or .tar.gz), so callers can pass several
      # distribution target directories after one flag
      shift
      while [[ $# -gt 0 && "$1" != --* ]]; do
        TARBALLS+=("$1")
        shift
      done
      ;;
    --out) OUT="$2"; shift 2 ;;
    --repo) REPO="$2"; shift 2 ;;
    --third-party) THIRD_PARTY="$2"; shift 2 ;;
    --collect) COLLECT=1; shift ;;
    --verify-only) VERIFY_DIR="$2"; MODE="verify"; shift 2 ;;
    --keys) KEYS="$2"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "$PROG: unknown argument: $1" >&2; usage ;;
  esac
done
[[ -z "$MODE" ]] && MODE="release"

if [[ "$MODE" == "verify" ]]; then
  [[ -n "$VERIFY_DIR" ]] || { echo "$PROG: --verify-only needs a directory" >&2; exit 2; }
  [[ -d "$VERIFY_DIR" ]] || { echo "$PROG: no such directory: $VERIFY_DIR" >&2; exit 2; }
  # default the verification target to the RC directory's tarballs,
  # unless the caller declared exactly which artifacts to gate
  if [[ "${#TARBALLS[@]}" -eq 0 ]]; then
    TARBALLS=("$VERIFY_DIR")
  fi
elif [[ "${#TARBALLS[@]}" -eq 0 ]]; then
  echo "$PROG: release mode needs at least one --tarballs" >&2
  usage
fi

# --- repo root ----------------------------------------------------------------

if [[ -z "$REPO" ]]; then
  REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"
fi
if [[ ! -f "$REPO/LICENSE-binary" ]]; then
  echo "$PROG: not a license-tooling repo checkout (no LICENSE-binary): $REPO" >&2
  echo "      pass the release-tag checkout with --repo" >&2
  exit 2
fi
CUR="$REPO/dev-support/licenses"
CURATED="$CUR/curation"
for f in archive-jar-mappings assembly-includes shaded-jars \
         manifest-assets.txt notice-aliases notice-exceptions \
         licenses-binary-aliases texts-generic manifest-template.txt; do
  if [[ ! -e "$CURATED/$f" ]]; then
    echo "$PROG: incomplete tooling checkout: missing $CURATED/$f" >&2
    exit 2
  fi
done
if [[ ! -d "$CUR/scripts" ]]; then
  echo "$PROG: incomplete tooling checkout: missing $CUR/scripts/" >&2
  exit 2
fi
if [[ ! -d "$CUR/texts" ]]; then
  echo "$PROG: incomplete tooling checkout: missing $CUR/texts/" >&2
  exit 2
fi
if [[ ! -f "$REPO/NOTICE-binary" ]]; then
  echo "$PROG: missing $REPO/NOTICE-binary (input of the NOTICE coverage gate)" >&2
  exit 2
fi

# --- shared state -------------------------------------------------------------

WORK=""
FIXES=()          # failed-check fix-list lines
FAILED=0
NOTE=""           # informational notes for the final summary

record_failure() { # <check-id> <fix-line>
  FAILED=1
  FIXES+=("$1|$2")
}

fix_for() { # <check-id> -> config-file guidance; ids are the stable
# check terms the gate scripts echo
  case "$1" in
    jar-naming) echo "add a curated mapping to dev-support/licenses/curation/archive-jar-mappings" ;;
    coverage) echo "regenerate LICENSE-binary (re-run this driver; commit the generated file); text aliases in dev-support/licenses/curation/licenses-binary-aliases" ;;
    inventory) echo "curate the jar as an assembly include in dev-support/licenses/curation/assembly-includes" ;;
    exclusions|exclusion-pattern) echo "fix the excludedArtifacts pattern in the root pom.xml" ;;
    versions) echo "informational: regenerate LICENSE-binary (re-run this driver)" ;;
    GATE) echo "the archive gate failed without a recognized requirement tag — inspect the gate log" ;;
    drift-gen) echo "the manifest could not be generated from the archives: a shipped jar resists resolution (same fix as jar-naming: dev-support/licenses/curation/archive-jar-mappings) or the inventory lacks a dependency (see the generate log)" ;;
    drift) echo "the committed LICENSE-binary drifted: commit the regenerated file this driver produced (out/generated-LICENSE-binary)" ;;
    texts) echo "the shipped licenses-binary/ differs from the generated texts: re-run this driver and ship its licenses-binary/ output" ;;
    notice) echo "add a NOTICE-binary attribution, a component text carrying the copyright line, or a justified row in dev-support/licenses/curation/notice-exceptions (drop stale rows)" ;;
    license-txt) echo "fix the LICENSE.txt / asset-manifest mismatch (see the check log for the exact rows)" ;;
    assets) echo "fix the asset finding: dev-support/licenses/curation/manifest-assets.txt, archive-asset-allowlist, or archive-asset-pin-exceptions" ;;
    enumeration) echo "the archive could not be enumerated: every jar must be nameable — inspect the enumeration log" ;;
    checksums) echo "regenerate the .sha512 checksum file for the artifact" ;;
    signatures) echo "signature does not verify against the KEYS file: do not trust this artifact" ;;
    rc-artifacts) echo "the RC must carry THIRD-PARTY.txt (the proposal's extra artifacts) for verification" ;;
    collection) echo "produce the dependency inventory: mvn -Plicenses org.codehaus.mojo:license-maven-plugin:aggregate-add-third-party (or pass --third-party <file>)" ;;
    *) echo "see the check log for details" ;;
  esac
}

run_check() { # <check-id> <label> <logfile> <cmd...>
  local id="$1" label="$2" logf="$3"; shift 3
  echo "--- $label"
  if "$@" > "$logf" 2>&1; then
    echo "    PASS ($(basename "$logf"))"
    return 0
  else
    local rc=$?
    echo "    FAIL (details: $(basename "$logf"))"
    record_failure "$id" "$(fix_for "$id")"
    return "$rc"
  fi
}

# --- tarball collection ---------------------------------------------------------
# Distribution tarballs only: module staging tars (hadoop-common-<v>.tar.gz
# and friends) are build intermediates with no jars and are never gated.
# The cloud-storage build produces a DIRECTORY assembly — tar it first, as
# the CI workflow does.

collect_tarballs() {
  local src name out_tar
  for src in "${TARBALLS[@]}"; do
    if [[ -d "$src" ]]; then
      # cloud-storage dir-format assembly: tar it so the gate treats it
      # as the distributed archive it is (same as the CI workflow does)
      for d in "$src"/hadoop-cloud-storage-*/; do
        if [[ -d "$d" ]]; then
          tar -czf "${d%/}.tar.gz" -C "$(dirname "$d")" "$(basename "$d")"
        fi
      done
      for d in "$src"/hadoop-cloud-storage*/target/hadoop-cloud-storage-*/; do
        if [[ -d "$d" ]]; then
          tar -czf "${d%/}.tar.gz" -C "$(dirname "$d")" "$(basename "$d")"
        fi
      done
      local found_any=0
      for out_tar in "$src"/*.tar.gz; do
        [[ -e "$out_tar" ]] || continue
        found_any=1
        case "$(basename "$out_tar")" in
          *-src.tar.gz)
            NOTE+="- source archive (no jars, not jar-gated): $(basename "$out_tar")"$'\n'
            ;;
          *) DIST_TARS+=("$out_tar") ;;
        esac
      done
      [[ "$found_any" -eq 1 ]] || { echo "$PROG: no .tar.gz under '$src'" >&2; exit 2; }
    elif [[ -f "$src" ]]; then
      case "$(basename "$src")" in
        *-src.tar.gz)
          NOTE+="- source archive (no jars, not jar-gated): $(basename "$src")"$'\n'
          ;;
        *.tar.gz) DIST_TARS+=("$src") ;;
        *) echo "$PROG: '$src' is not a .tar.gz" >&2; exit 2 ;;
      esac
    else
      echo "$PROG: tarball path not found: $src" >&2
      exit 2
    fi
  done
}

enumerate_one() { # <tarball> <workdir-for-this-archive>
  local tar="$1" dir="$2"
  mkdir -p "$dir/untarred"
  tar -xzf "$tar" -C "$dir/untarred"
  python3 "$SCRIPT_DIR/scripts/enumerate-archive-jars.py" \
    "$dir/untarred" -o "$dir/archive-jars.tsv"
}

find_shipped_manifest() { # <untarred-dir> -> path or empty
  find "$1" -maxdepth 2 -name LICENSE-binary -print -quit
}

find_shipped_licenses_dir() { # <untarred-dir> -> path or empty
  find "$1" -maxdepth 2 -type d -name licenses-binary -print -quit
}

generate_manifest() { # <output-file> <tsv...>
  local out="$1"; shift
  python3 "$SCRIPT_DIR/scripts/generate-archive-manifest.py" \
    --inventory "$THIRD_PARTY" \
    --mappings "$CURATED/archive-jar-mappings" \
    --shaded "$CURATED/shaded-jars" \
    --assets "$CURATED/manifest-assets.txt" \
    --template "$CURATED/manifest-template.txt" \
    --archive-tsv "$@" \
    --out "$out"
}

generate_licenses_dir() { # <manifest> <output-dir>
  python3 "$SCRIPT_DIR/scripts/generate-archive-manifest.py" \
    --emit-licenses-dir-only --manifest "$1" \
    --store "$CUR/texts" \
    --aliases "$CURATED/licenses-binary-aliases" \
    --generic "$CURATED/texts-generic" \
    --licenses-dir "$2"
}

sha512_of() { # <file> -> "<hash>  <basename>" line (sha512sum -c compatible)
  local f="$1"
  echo "$(sha512sum "$f" | cut -d' ' -f1)  $(basename "$f")"
}

# =============================================================================
# Main
# =============================================================================

if [[ "$MODE" == "verify" ]]; then
  OUT="${OUT:-$(mktemp -d "$VERIFY_DIR/.verify-output.XXXXXX")}"
else
  OUT="${OUT:-$REPO/target/release-checks}"
fi
mkdir -p "$OUT/checks"
echo "License release checks — output: $OUT"

# the dependency inventory: release mode resolves it from --third-party,
# --collect or the repo's pre-collected file; verification mode takes the
# copy the RC carries (one of the proposal's two extra artifacts)
if [[ "$MODE" == "verify" ]]; then
  if [[ -f "$VERIFY_DIR/THIRD-PARTY.txt" ]]; then
    THIRD_PARTY="$VERIFY_DIR/THIRD-PARTY.txt"
  else
    echo "FAIL: $VERIFY_DIR carries no THIRD-PARTY.txt — the RC must ship the proposal's extra artifacts for verification" >&2
    record_failure "rc-artifacts" "$(fix_for rc-artifacts)"
    THIRD_PARTY=""
  fi
elif [[ -z "$THIRD_PARTY" ]]; then
  # default provenance: the repo's own collected inventory
  if [[ -f "$REPO/target/third-party/THIRD-PARTY.txt" ]]; then
    THIRD_PARTY="$REPO/target/third-party/THIRD-PARTY.txt"
    echo "THIRD-PARTY: using $THIRD_PARTY (pre-collected; pass --collect to regenerate)"
  elif [[ "$COLLECT" -eq 1 ]]; then
    THIRD_PARTY="$REPO/target/third-party/THIRD-PARTY.txt"
    echo "THIRD-PARTY: collecting with the licenses Maven profile..."
    (cd "$REPO" && mvn -B -ntp -Plicenses -DskipTests \
       org.codehaus.mojo:license-maven-plugin:aggregate-add-third-party) \
      || { echo "$PROG: THIRD-PARTY collection failed" >&2; exit 1; }
  else
    echo "FAIL: no THIRD-PARTY inventory. Either build it first:" >&2
    echo "  mvn -B -ntp -Plicenses -DskipTests org.codehaus.mojo:license-maven-plugin:aggregate-add-third-party" >&2
    echo "or pass --third-party <file> / --collect" >&2
    exit 1
  fi
fi
if [[ -n "$THIRD_PARTY" && -s "$THIRD_PARTY" ]]; then
  :
elif [[ "$MODE" == "release" ]]; then
  echo "FAIL: THIRD-PARTY inventory is missing or empty: $THIRD_PARTY" >&2
  record_failure "collection" "$(fix_for collection)"
  THIRD_PARTY=""
elif [[ -n "$THIRD_PARTY" ]]; then
  echo "FAIL: the RC's THIRD-PARTY.txt is empty: $THIRD_PARTY" >&2
  record_failure "rc-artifacts" "$(fix_for rc-artifacts)"
  THIRD_PARTY=""
fi

if [[ -n "$KEYS" && ! -f "$KEYS" ]]; then
  echo "$PROG: KEYS file not found: $KEYS" >&2
  exit 2
fi

collect_tarballs
if [[ "${#DIST_TARS[@]}" -eq 0 ]]; then
  echo "FAIL: no binary distribution tarball found to gate" >&2
  exit 2
fi

# ---- per-archive gates ---------------------------------------------------------

TSVS=()
for tar in "${DIST_TARS[@]}"; do
  name="$(basename "$tar" .tar.gz)"
  dir="$OUT/checks/$name"
  rm -rf "$dir"
  mkdir -p "$dir"
  echo ""
  echo "== $name"

  if ! run_check "enumeration" "enumerate every shipped jar" "$dir/enumerate.log" \
      enumerate_one "$tar" "$dir"; then
    continue   # nothing downstream can run on an unenumerable archive
  fi
  TSVS+=("$dir/archive-jars.tsv")

  lb="$(find_shipped_manifest "$dir/untarred")"
  if [[ -z "$THIRD_PARTY" ]]; then
    echo "    gate skipped: no THIRD-PARTY inventory in the RC (already recorded as MISSING-ARTIFACTS)"
  elif [[ -z "$lb" ]]; then
    echo "    coverage FAIL: $name ships no LICENSE-binary — every jar in it is unlicensed"
    echo "coverage FAILURE: $tar ships no LICENSE-binary — every jar in it is unlicensed" > "$dir/gate.log"
    record_failure "coverage" "$(fix_for coverage)"
  else
    # the gate checks several requirements at once; on failure, attribute
    # the fix-list entries to the exact requirement(s) the log names
    if run_check "GATE" "coverage and inventory gates (every shipped jar licensed, on the inventory, nameable, exclusions clean)" \
        "$dir/gate.log" \
        "$SCRIPT_DIR/scripts/check-archive-licenses.sh" \
        "$dir/archive-jars.tsv" "$lb" "$THIRD_PARTY" "$REPO"; then
      :
    else
      # the gate log's requirement tokens (R-IDs, stable output of the
      # check script) map to the CI gate terms humans see
      declare -A term_map=([R36]=jar-naming [R38]=coverage [R26]=inventory [R39]=exclusions [R47]=exclusion-pattern)
      matched=0
      for tok in R36 R38 R26 R39 R47; do
        if grep -q "^$tok FAILURE" "$dir/gate.log"; then
          record_failure "${term_map[$tok]}" "$(fix_for "${term_map[$tok]}")"
          matched=1
        fi
      done
      if [[ "$matched" -eq 1 ]]; then
        # drop the generic GATE entry run_check recorded: the specific
        # requirement terms above are what the fix list should carry
        idx=-1
        for i in "${!FIXES[@]}"; do
          [[ "${FIXES[$i]}" == "GATE|"* ]] && idx=$i
        done
        if [[ "$idx" -ge 0 ]]; then unset "FIXES[$idx]"; fi
      fi
    fi
  fi

  # R7 stage C asset gate: main distribution only (the cloud dist ships
  # SDK jars, no web assets; the npm probes need the UI WARs)
  case "$name" in
    hadoop-cloud-storage-*) ;;
    *)
      run_check "assets" "asset gate (non-jar assets, loose and WAR-embedded)" \
        "$dir/assets.log" \
        python3 "$SCRIPT_DIR/scripts/check-archive-assets.py" \
          "$dir/untarred" "$REPO" --tsv "$dir/archive-assets.tsv" \
          --manifest "$CURATED/manifest-assets.txt" \
          --allowlist "$CURATED/archive-asset-allowlist" \
          --ui-trees "$CURATED/ui-archive-trees" \
          --pin-exceptions "$CURATED/archive-asset-pin-exceptions"
      ;;
  esac
done

# ---- generation + drift gates (R40/R53/R14) ------------------------------------

if [[ "${#TSVS[@]}" -gt 0 && -n "$THIRD_PARTY" ]]; then
  echo ""
  echo "== generation and drift"
  GEN="$OUT/generated-LICENSE-binary"
  if run_check "drift-gen" "generate LICENSE-binary from this run's archives" \
      "$OUT/checks/generate.log" \
      generate_manifest "$GEN" "${TSVS[@]}"; then

    if [[ "$MODE" == "verify" ]]; then
      # EVERY binary tarball ships the manifest of record: diff each
      # one against the manifest regenerated from all archives. Checking
      # only the first tarball would let a stale hand-edited copy ride
      # in any of the others. (A tarball that ships no LICENSE-binary at
      # all is already failed by the archive gate above.)
      for tar in "${DIST_TARS[@]}"; do
        name="$(basename "$tar" .tar.gz)"
        shipped="$(find_shipped_manifest "$OUT/checks/$name/untarred")"
        [[ -n "$shipped" ]] || continue
        if run_check "drift" "license drift gate ($name: shipped manifest == generated from the archives)" \
            "$OUT/checks/drift-$name.log" diff -u "$shipped" "$GEN"; then
          echo "    drift gate ($name): the shipped manifest matches this run's archives"
        fi
      done
    else
      if run_check "drift" "license drift gate (manifest of record == generated from the archives)" \
          "$OUT/checks/drift.log" diff -u "$REPO/LICENSE-binary" "$GEN"; then
        echo "    drift gate: the manifest of record matches this run's archives"
      fi
    fi

    rm -rf "$OUT/licenses-binary"
    if run_check "bundling" "generate licenses-binary/ from the generated manifest" \
        "$OUT/checks/generate-licenses-dir.log" \
        generate_licenses_dir "$GEN" "$OUT/licenses-binary"; then
      for tar in "${DIST_TARS[@]}"; do
        name="$(basename "$tar" .tar.gz)"
        shipped="$(find_shipped_licenses_dir "$OUT/checks/$name/untarred")"
        if [[ -z "$shipped" ]]; then
          echo "    note: $name ships no licenses-binary/ — the generated directory joins the RC bundle"
          NOTE+="- $name shipped no licenses-binary/; the generated one is in the bundle"$'\n'
        else
          if run_check "texts" "license texts gate: shipped == generated ($name)" \
              "$OUT/checks/r53-$name.log" diff -r "$shipped" "$OUT/licenses-binary"; then
            :
          fi
        fi
      done
    fi
  fi

  # R14 NOTICE coverage at release time: the same check the per-push
  # graph gate runs, re-executed against the committed NOTICE-binary and
  # the manifest of record. Run through the check-asset-manifest module
  # with the curation paths of THIS checkout (--repo), so a verification
  # checkout of the release tag is self-consistent. The battery's other
  # gates (R48, the texts-store R53) remain per-push graph gates: they
  # validate the committed source tree, not the RC.
  if [[ "$MODE" == "release" ]]; then
    run_check "notice" "NOTICE coverage gate (against the committed state)" \
      "$OUT/checks/notice.log" \
      python3 - "$SCRIPT_DIR/scripts/check-asset-manifest.py" "$CUR" "$REPO" <<'PY'
import importlib.util, sys
spec = importlib.util.spec_from_file_location("cam", sys.argv[1])
cam = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cam)
cur, repo = sys.argv[2], sys.argv[3]
cam.NOTICE_ALIASES = cur + "/curation/notice-aliases"
cam.NOTICE_EXCEPTIONS = cur + "/curation/notice-exceptions"
cam.ALIASES = cur + "/curation/licenses-binary-aliases"
cam.TEXTS_GENERIC = cur + "/curation/texts-generic"
cam.TEXTS_STORE = cur + "/texts"
failures = cam.check_notice_coverage(repo)
for f in failures:
    print("FAIL: " + f, file=sys.stderr)
sys.exit(1 if failures else 0)
PY
  else
    NOTE+="- NOTICE and committed-state gates: verification mode checks the RC, not the repo; run release mode on a checkout for those"$'\n'
  fi
fi

# ---- voter-mode extra: checksums and signatures ---------------------------------

if [[ "$MODE" == "verify" ]]; then
  echo ""
  echo "== artifact integrity"
  for tar in "${DIST_TARS[@]}"; do
    name="$(basename "$tar")"
    sumfile="$VERIFY_DIR/$name.sha512"
    if [[ -f "$sumfile" ]]; then
      # accept both spellings real releases use: "<hash>  <name>"
      # (sha512sum -c format) and "SHA512 (name) = hash" (sha512sum --tag,
      # the format create-release's signing step writes). Whatever the
      # wrapper, the digest itself is the 128-hex-char token; an empty
      # match fails the comparison below.
      want="$(grep -oE '[0-9a-fA-F]{128}' "$sumfile" | head -1)"
      have="$(sha512sum "$tar" | cut -d' ' -f1)"
      if [[ "$want" == "$have" ]]; then
        echo "    sha512 OK: $name"
      else
        echo "    SHA512 FAIL: $name (expected $want, got $have)"
        record_failure "checksums" "$(fix_for checksums)"
      fi
    else
      echo "    SHA512 FAIL: no .sha512 for $name in $VERIFY_DIR"
      record_failure "checksums" "$(fix_for checksums)"
    fi
    if [[ -n "$KEYS" ]]; then
      # verify against the project's public KEYS: import once, then
      # gpg --verify each detached signature
      if ! gpg --import "$KEYS" > "$OUT/checks/gpg-import.log" 2>&1; then
        echo "    GPG FAIL: cannot import the KEYS file (details: gpg-import.log)"
        record_failure "signatures" "$(fix_for signatures)"
      fi
      if [[ -f "$VERIFY_DIR/$name.asc" ]]; then
        if gpg --verify "$VERIFY_DIR/$name.asc" "$tar" > "$OUT/checks/gpg-$name.log" 2>&1; then
          echo "    gpg OK: $name"
        else
          echo "    GPG FAIL: $name (details: gpg-$name.log)"
          record_failure "signatures" "$(fix_for signatures)"
        fi
      else
        echo "    GPG FAIL: no detached signature for $name"
        record_failure "signatures" "$(fix_for signatures)"
      fi
    fi
  done
fi

# ---- RC bundle (release mode, green only) ---------------------------------------

BUNDLE=""
if [[ "$MODE" == "release" && "$FAILED" -eq 0 ]]; then
  BUNDLE="$OUT/rc-bundle"
  mkdir -p "$BUNDLE"
  cp "$THIRD_PARTY" "$BUNDLE/THIRD-PARTY.txt"
  cp "$GEN" "$BUNDLE/LICENSE-binary.generated"
  cp -r "$OUT/licenses-binary" "$BUNDLE/licenses-binary"
  : > "$BUNDLE/SHA512SUMS"
  for tar in "${DIST_TARS[@]}"; do
    sha512_of "$tar" >> "$BUNDLE/SHA512SUMS"
  done

  version="$(basename "${DIST_TARS[0]}" .tar.gz)"; version="${version#hadoop-}"
  cat > "$BUNDLE/CHECKLIST.md" <<EOF
# RM offline checklist — what remains after the license step

The license step verified the tarballs in \`$(basename "${DIST_TARS[0]}")\`'s
directory and produced this bundle. The remaining steps are exactly
HowToRelease's, in order; nothing license-related is left.

1. Verify the checksums in this bundle (\`SHA512SUMS\`) against the
   tarballs: \`sha512sum -c SHA512SUMS\` from the tarball directory.
2. Sign each artifact with the release key:
   \`gpg --detach-sign --armor -u <release-key> <artifact>\`
   (tarballs; add THIRD-PARTY.txt if your release custom signs it).
3. Copy the tarballs, \`.asc\`, \`.sha512\`, plus this bundle's
   \`THIRD-PARTY.txt\`, \`LICENSE-binary.generated\` and \`licenses-binary/\`
   to the release directory and the SVN dev staging location
   (\`hadoop-dist/dev/\`), replacing the in-tarball license files with the
   generated ones if they differ (they do not — the drift gate is green).
4. Stage the module jars to Nexus and close the staging repository —
   unchanged from today (\`create-release --asfrelease\`, or manual).
5. Create and push the signed RC tag (\`rel/release-X.Y.Z-RCn\`).
6. Send the vote email; add one line linking this verification (the
   \`out/\` directory or the CI run that executed it) and noting the two
   extra artifacts: THIRD-PARTY.txt and the generated license files.
EOF
fi

# ---- verdict ---------------------------------------------------------------------

echo ""
if [[ "$FAILED" -ne 0 ]]; then
  echo "=============================================="
  echo "RELEASE LICENSE CHECKS: RED — fix list"
  echo "=============================================="
  : > "$OUT/fixlist.txt"
  for entry in "${FIXES[@]}"; do
    id="${entry%%|*}"; fix="${entry#*|}"
    echo "  [$id] $fix" | tee -a "$OUT/fixlist.txt"
  done
  echo ""
  echo "Per-check logs with the exact failing entries: $OUT/checks/"
  exit 1
fi

echo "=============================================="
echo "RELEASE LICENSE CHECKS: GREEN"
echo "=============================================="
echo "Gates passed: coverage, inventory, jar-naming, exclusions (archive) | assets | drift | texts | notice"
if [[ -n "$NOTE" ]]; then
  echo "Notes:"
  printf '%s' "$NOTE"
fi
if [[ -n "$BUNDLE" ]]; then
  echo "RC artifact set: $BUNDLE"
  echo "  THIRD-PARTY.txt, LICENSE-binary.generated, licenses-binary/, SHA512SUMS, CHECKLIST.md"
  echo "Next: follow $BUNDLE/CHECKLIST.md (sign, stage, tag, vote)."
fi
exit 0
