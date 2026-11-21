<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->
# Release-time license checks — what changes for a release

This directory contains the automated license tooling introduced by
HADOOP-20078. This file is the operator's view: what the release
manager and voters do differently once it is adopted. The technical
detail lives in the header comments of the individual tools.

**In one sentence:** one opt-in step runs inside the RC build, the RC
carries two extra artifacts, and `hadoop-vote.sh` gains one check.
Everything else about releasing Hadoop is unchanged.

## Building the RC: one opt-in flag

Run `create-release` exactly as today, adding `--license-check`:

    dev-support/bin/create-release --asfrelease --license-check ...

Inside that build, after the tarballs are assembled and before anything
is signed or staged, the license step
(`dev-support/licenses/run-release-checks.sh`) runs on the exact
tarballs being produced. It:

1. lists every jar the archives ship — including jars inside WARs — and
   resolves each to its Maven name (jars it cannot name fail the build;
   they are never silently skipped);
2. collects the dependency report (THIRD-PARTY.txt: every dependency
   with its license names and versions, from the opt-in `licenses`
   Maven profile);
3. generates LICENSE-binary and the `licenses-binary/` text directory
   from those two lists and the version-controlled license-text store;
4. verifies, and fails the build unless: every shipped jar is covered
   by the license file; everything listed actually ships (or is inside
   a shaded jar, or has a recorded reason); nothing excluded from the
   build still ships; the shipped license texts match the license file;
   and the committed file equals the freshly generated one — so a
   hand-edited or stale license file cannot ride into an RC.

GREEN: the check writes its RC artifact set to
`target/artifacts/license-checks/` — THIRD-PARTY.txt, the generated
LICENSE-binary, `licenses-binary/`, sha512 checksums and a short RM
checklist. RED: the build stops before signing with a fix list that
names the exact failing entries and the configuration file where each
change belongs.

## When the step fails

The failure message names the exact entries. The fix is never editing
the license files by hand: it is a small change in a reviewed
configuration file under this directory's `curation/` subdirectory (a
jar-name mapping, a dependency exception, a text alias), normally made
in the pull request that introduced the dependency. Then re-run the
build. Adapting the system
to a release line is bounded and measured: it is one collection plus
one strict license-name resolution (minutes; the method is recorded in
HADOOP-20078), not a per-release license project.

## Publishing the RC: two extra artifacts

The verified RC carries, alongside the usual tarball, signature and
checksum set:

- **THIRD-PARTY.txt** — every shipped dependency with its license and
  version;
- the **generated LICENSE-binary** and **licenses-binary/** that match
  the tarball exactly.

Copy them from `target/artifacts/license-checks/` into the RC staging
directory next to the other artifacts, and add one line to the vote
email pointing at them, for example:

    License verification artifacts (THIRD-PARTY.txt and the generated
    license files) are staged with this RC; they were generated from,
    and verified against, the exact tarballs being voted on.

## Voting: one more check

`dev-support/hadoop-vote.sh` now runs the license verification as part
of the standard vote (item 6). A voter can also run it directly against
a staged or downloaded RC:

    dev-support/licenses/run-release-checks.sh --verify-only <rc-dir> \
        --repo <checkout-of-the-release-tag> [--keys KEYS]

It re-enumerates each binary tarball and re-runs the full gate against
the tarball's own shipped LICENSE-binary and the RC's THIRD-PARTY.txt,
including the drift check (the shipped manifest must equal the manifest
regenerated from the archive), verifying checksums and GPG signatures
when `--keys` is given. A red result here means the staged RC's license
files do not match its own contents; a signed RC cannot be patched, so
it must be replaced by a new RC.

For RCs cut before this tooling existed, `hadoop-vote.sh` reports the
license check as skipped rather than failed.

## After the vote: nothing new

The files were generated from, and verified against, the tarballs being
voted on, so what was verified is exactly what gets promoted. The next
RC build re-verifies everything from scratch: a dependency added since
the last release fails the build until it is listed (and the build says
what to add); a component removed stops being listed at the next
generation.

## Proposed HowToRelease (wiki) edits

The release guide is maintained on the project wiki; the corresponding
text changes for adoption are:

1. In the "Build the release candidate" step: "Run create-release with
   the additional --license-check flag. This runs the license checks on
   the exact tarballs produced; a failure stops the build before
   signing with instructions naming the entries to fix (always a small
   configuration change, never a hand-edited license file)."
2. In the staging step: "Also stage THIRD-PARTY.txt and the generated
   LICENSE-binary / licenses-binary/ directory from
   target/artifacts/license-checks/ with the RC."
3. In the vote email template: "add one line pointing at the license
   verification artifacts staged with the RC."

## Scope notes

- NOTICE-binary is NOT generated: its attribution corpus is curated
  prose. The step checks its coverage (every attribution-required
  component is mentioned, covered by a component text, or carries a
  recorded justification) but never rewrites it.
- The unused-jar reports in this directory are informational
  (reflection, ServiceLoader and native loading keep their verdicts
  conservative); they never gate a release, and dropping a jar stays a
  normal, reviewed dependency change.
