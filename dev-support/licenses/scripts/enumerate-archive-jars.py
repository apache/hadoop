#!/usr/bin/env python3
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
"""Enumerate every jar in an assembled distribution archive.

Normalizes each jar to g:a:v, preferring embedded metadata over filename
parsing:

  1. META-INF/maven pom.properties matching the jar's own filename
     (artifact-version.jar) -> full g:a:v (origin pom.properties).
     A single pom.properties is trusted only when the filename carries
     no version to check it against. A jar whose pom.properties all
     mismatch its filename is a shaded jar (hadoop-client-runtime
     carries 71): it is NEVER attributed to an embedded artifact — the
     archive's view is the top-level jar (requirement 37) — and
     normalization falls through:
  2. META-INF/MANIFEST.MF                 -> artifactId/version only
     (Implementation-Title/Bundle-SymbolicName, Implementation-Version/
     Bundle-Version); the group is unknown, the row is partial and goes
     to the manual-mapping worklist (origin manifest)
  3. filename                             -> artifactId/version guess from
     the last version-looking -segment; group unknown, partial row
     (origin filename)
  4. nothing                              -> unmapped row, manual mapping

Partial and unmapped rows are resolved by the curated
dev-support/licenses/curation/archive-jar-mappings file (stem -> g:a) when the
gate runs.

Jars nested inside WARs are enumerated too (requirement 27): their path is
reported as <war>#<inner path>. Output is a TSV on stdout (or --output):

  <archive path>\t<group>\t<artifact>\t<version>\t<origin>

Rows with an empty group are partial; rows with empty group and artifact are
unmapped. Neither is silently dropped — check-archive-licenses.sh fails on
them until they are curated. Note: for a tarball input the paths are
relative to its single top-level directory; for an exploded directory they
are relative to that directory itself.

Usage: enumerate-archive-jars.py <tarball.tar.gz | exploded-dir> [-o out.tsv]
"""

import argparse
import io
import os
import re
import shutil
import sys
import tarfile
import tempfile
import zipfile


def parse_pom_properties(zf):
    """All g:a:v candidates from META-INF/maven/*/*/pom.properties.

    Shaded jars embed the pom.properties of the artifacts they relocate
    (hadoop-client-runtime carries 71 of them), so a jar can have many;
    the caller picks the right one.
    """
    found = []
    for name in zf.namelist():
        if name.startswith("META-INF/maven/") and name.endswith("/pom.properties"):
            props = {}
            with zf.open(name) as fh:
                for raw in fh.read().decode("utf-8", "replace").splitlines():
                    line = raw.strip()
                    if "=" in line and not line.startswith("#"):
                        key, _, value = line.partition("=")
                        props[key.strip()] = value.strip()
            g = props.get("groupId", "")
            a = props.get("artifactId", "")
            v = props.get("version", "")
            if g and a and v:
                found.append((g, a, v))
    return found


def filename_artifact(path):
    """Best-effort artifactId from the jar filename (artifact-version.jar)."""
    hit = parse_filename(path)
    return hit[1] if hit else ""


def parse_manifest(zf):
    """artifactId/version from MANIFEST main attributes; group unknown."""
    try:
        with zf.open("META-INF/MANIFEST.MF") as fh:
            data = fh.read().decode("latin-1")
    except KeyError:
        return None
    # unfold continuation lines (leading space)
    lines = []
    for raw in data.splitlines():
        if raw.startswith((" ", "\t")) and lines:
            lines[-1] += raw[1:]
        else:
            lines.append(raw)
    attrs = {}
    for line in lines:
        if ":" in line:
            key, _, value = line.partition(":")
            attrs[key.strip()] = value.strip()
    artifact = attrs.get("Implementation-Title") or attrs.get("Bundle-SymbolicName") or ""
    version = attrs.get("Implementation-Version") or attrs.get("Bundle-Version") or ""
    if artifact:
        # OSGi symbolic names may carry directives: foo.bar;version="1.0"
        artifact = artifact.split(";")[0]
        return "", artifact, version, "manifest"
    return None


VERSION_SEGMENT = re.compile(r"^[0-9]")


def parse_filename(path):
    """artifactId/version guess from the last version-looking -segment.

    Scans right-to-left for the first segment starting with a digit, then
    keeps extending left while the preceding segment also starts with a
    digit — hyphenated versions (zstd-jni 1.5.7-8) must stay in the
    version, not bleed into the artifactId.
    """
    base = os.path.basename(path)
    if base.lower().endswith(".jar"):
        base = base[: -len(".jar")]
    parts = base.split("-")
    for i in range(len(parts) - 1, 0, -1):
        if VERSION_SEGMENT.match(parts[i]):
            while i > 1 and VERSION_SEGMENT.match(parts[i - 1]):
                i -= 1
            return "", "-".join(parts[:i]), "-".join(parts[i:]), "filename"
    return None


def normalize(path, zf):
    """g, a, v, origin for one opened jar (zipfile).

    Priority (requirement 36, extended by the shaded-jar policy of
    requirement 37): the pom.properties matching the jar's own filename;
    then the only pom.properties if there is exactly one; a jar with
    several embedded pom.properties and no filename match is a shaded
    jar — it is never attributed to an embedded artifact, but falls
    through to the top-level jar's manifest and then the filename.
    """
    stem = filename_artifact(path)
    candidates = parse_pom_properties(zf)
    if candidates:
        for g, a, v in candidates:
            if stem and a.lower() == stem.lower():
                return g, a, v, "pom.properties"
        if len(candidates) == 1 and not stem:
            # no version in the filename to check the candidate against;
            # a single pom.properties is the only identity available
            g, a, v = candidates[0]
            return g, a, v, "pom.properties"
        # no candidate matches the jar's own filename: a shaded jar —
        # never attribute it to an embedded artifact (R37); fall through
    hit = parse_manifest(zf)
    fn = parse_filename(path)
    # a shaded jar's manifest can be the embedded artifact's (the
    # hadoop-client-runtime jar carries "Apache Hadoop shaded Protobuf"):
    # trust the manifest only when it agrees with the filename, or when
    # the filename yields no stem; the filename is the top-level identity
    if hit:
        if not stem or hit[1].lower() == stem.lower():
            return hit
    if fn:
        return fn
    if hit:
        return hit
    return "", "", "", "unmapped"


def walk_jars(zf, prefix):
    """Yield (path-inside-zip, zipfile handle) for every jar in a zip
    (a WAR is a zip; nested WARs are not a thing in the dist)."""
    for name in zf.namelist():
        if name.endswith("/"):
            continue
        lower = name.lower()
        if lower.endswith(".jar"):
            with zf.open(name) as fh:
                yield f"{prefix}#{name}", zipfile.ZipFile(io.BytesIO(fh.read()))
        elif lower.endswith(".war"):
            with zf.open(name) as fh:
                yield from walk_jars(zipfile.ZipFile(io.BytesIO(fh.read())), f"{prefix}#{name}")


def main():
    ap = argparse.ArgumentParser(
        description="Enumerate every jar in an assembled distribution archive and normalize to g:a:v (requirement 36)."
    )
    ap.add_argument("input", help="distribution .tar.gz, or an exploded directory")
    ap.add_argument("-o", "--output", help="TSV output file (default stdout)")
    args = ap.parse_args()

    if not os.path.exists(args.input):
        sys.exit(f"ERROR: input does not exist: {args.input}")

    tmpdir = None
    root = args.input
    if os.path.isfile(root):
        tmpdir = tempfile.mkdtemp(prefix="archive-jars.")
        with tarfile.open(root) as tf:
            try:
                tf.extractall(tmpdir, filter="data")
            except TypeError:
                # python without the tarfile data filter (added in 3.12,
                # backported to 3.11.4/3.10.12/3.9.17): unfiltered
                # extraction of a tarball with "../" or absolute-path
                # members writes outside tmpdir. This tool also verifies
                # DOWNLOADED RCs, so the input is not always trusted:
                # fail closed instead of extracting unfiltered.
                sys.exit("ERROR: this python's tarfile lacks the data "
                         "filter (needs python >= 3.12, or >= 3.9.17 "
                         "with the security backport) and the input is "
                         "a tarball: " + root +
                         " — extract it yourself or pass an exploded "
                         "directory")
        # the tarball usually wraps everything in a single top dir
        entries = os.listdir(tmpdir)
        root = os.path.join(tmpdir, entries[0]) if len(entries) == 1 else tmpdir

    rows = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for fn in sorted(filenames):
            path = os.path.join(dirpath, fn)
            rel = os.path.relpath(path, root)
            lower = fn.lower()
            if lower.endswith(".jar"):
                try:
                    with zipfile.ZipFile(path) as zf:
                        rows.append((rel,) + normalize(rel, zf))
                except zipfile.BadZipFile as err:
                    print(f"WARN: not a jar (kept as unmapped): {rel}: {err}", file=sys.stderr)
                    rows.append((rel, "", "", "", "unmapped"))
            elif lower.endswith(".war"):
                try:
                    with zipfile.ZipFile(path) as zf:
                        for inner_path, inner_zf in walk_jars(zf, rel):
                            rows.append((inner_path,) + normalize(inner_path, inner_zf))
                except zipfile.BadZipFile as err:
                    print(f"WARN: not a war (kept as unmapped): {rel}: {err}", file=sys.stderr)
                    rows.append((rel, "", "", "", "unmapped"))

    out = open(args.output, "w") if args.output else sys.stdout
    try:
        for row in sorted(rows):
            out.write("\t".join(row) + "\n")
    finally:
        if out is not sys.stdout:
            out.close()
    if tmpdir:
        shutil.rmtree(tmpdir, ignore_errors=True)

    full = sum(1 for r in rows if r[1])
    partial = sum(1 for r in rows if not r[1] and r[2])
    unmapped = sum(1 for r in rows if not r[2])
    print(
        f"enumerated {len(rows)} jar(s): {full} mapped via pom.properties, "
        f"{partial} partial (manifest/filename, no group), {unmapped} unmapped",
        file=sys.stderr,
    )
    if not rows:
        # a distribution with zero jars is a broken input, not a compliant
        # one — never let a mistyped path produce a green empty TSV
        sys.exit("ERROR: no jars found under the input — is it a distribution layout?")


if __name__ == "__main__":
    main()
