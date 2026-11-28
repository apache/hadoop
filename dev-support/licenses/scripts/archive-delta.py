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
"""Third-party jar delta between two archive ground-truth runs.

Compares the archive-jars.tsv files of a baseline run with the current
run's, archive by archive (matched by directory name), and prints what a
change — typically a pruning PR — did to the shipped third-party jars:

  - g:a:v   removed: no copy ships any more
  + g:a:v   added
  ~ g:a     version change (old -> new), then where each dropped
            ("    - v") and new ("    + v") version shipped
  # g:a:v   copy count change (where it ships: old -> new lib dirs)

Rows are read with the R40 generator's loader (archive-jar-mappings
promotion, own jars left out). Report only: exit 0 on any delta, 2 on
malformed input.

Usage: archive-delta.py <baseline-dir> <current-dir> [-o archive-delta.txt]

Each directory holds <archive name>/archive-jars.tsv, as the
archive-ground-truth artifact does under archive-ground-truth/. Archives
are matched by name without the release version (a version bump of
trunk still compares like for like).
"""

import argparse
import collections
import glob
import importlib.util
import os
import re
import sys

sys.dont_write_bytecode = True  # importlib load below must not litter the tree

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location(
    "generate_archive_manifest",
    os.path.join(SCRIPT_DIR, "generate-archive-manifest.py"))
_gam = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_gam)

RELEASE = re.compile(r"-(\d[\w.\-]*)$")  # hadoop-3.6.0-SNAPSHOT -> 3.6.0-SNAPSHOT


def archives(base):
    """archive name without its release version -> (name, TSV path), so
    hadoop-3.6.0-SNAPSHOT and hadoop-3.7.0-SNAPSHOT are the same archive."""
    out = {}
    for t in glob.glob(os.path.join(base, "*", "archive-jars.tsv")):
        name = os.path.basename(os.path.dirname(t))
        out[RELEASE.sub("", name)] = (name, t)
    return out


def layout(tsv, mappings):
    """(g:a, v) -> sorted lib dirs, for one archive."""
    rows, _unresolved = _gam.load_archive_rows([tsv], mappings)
    where = collections.defaultdict(set)
    for path, ga, v in rows:
        # drop the top-level directory and the release version it carries
        # (WAR file names repeat it), so a version bump is not a move
        top, _, rel = path.partition("/")
        m = RELEASE.search(top)
        if m and rel:
            rel = rel.replace(m.group(1), "<version>")
        rel = rel or path
        where[(ga, v)].add(rel.rsplit("/", 1)[0] if "/" in rel else ".")
    return {k: sorted(v) for k, v in where.items()}


def delta(old, new):
    out = []
    old_ga = collections.defaultdict(set)
    new_ga = collections.defaultdict(set)
    for ga, v in old:
        old_ga[ga].add(v)
    for ga, v in new:
        new_ga[ga].add(v)
    for ga in sorted(set(old_ga) | set(new_ga)):
        o, n = old_ga.get(ga, set()), new_ga.get(ga, set())
        if o and n and o != n:
            out.append("~ %s  %s -> %s" % (ga, ",".join(sorted(o)),
                                           ",".join(sorted(n))))
            # where each dropped or new version shipped / ships
            out.extend("    - %s  (%s)" % (v, ", ".join(old[(ga, v)]))
                       for v in sorted(o - n))
            out.extend("    + %s  (%s)" % (v, ", ".join(new[(ga, v)]))
                       for v in sorted(n - o))
        elif o and not n:
            out.extend("- %s:%s  (%s)" % (ga, v, ", ".join(old[(ga, v)]))
                       for v in sorted(o))
        elif n and not o:
            out.extend("+ %s:%s  (%s)" % (ga, v, ", ".join(new[(ga, v)]))
                       for v in sorted(n))
        for v in sorted(o & n):
            if old[(ga, v)] != new[(ga, v)]:
                out.append("# %s:%s  %s -> %s" % (
                    ga, v, ", ".join(old[(ga, v)]), ", ".join(new[(ga, v)])))
    return out


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("baseline")
    parser.add_argument("current")
    parser.add_argument("--mappings",
                        default=os.path.join(SCRIPT_DIR, "..", "curation", "archive-jar-mappings"))
    parser.add_argument("-o", "--output", default=None)
    args = parser.parse_args()

    old_a, new_a = archives(args.baseline), archives(args.current)
    if not new_a:
        _gam.fail("no <archive>/archive-jars.tsv under %s" % args.current)
    out = ["# Archive third-party jar delta: %s -> %s"
           % (args.baseline, args.current)]
    for key in sorted(set(old_a) | set(new_a)):
        out.append("")
        if key not in old_a:
            out.append("## %s: new archive (no baseline)" % new_a[key][0])
            continue
        if key not in new_a:
            out.append("## %s: archive no longer built" % old_a[key][0])
            continue
        (old_name, old_tsv), (new_name, new_tsv) = old_a[key], new_a[key]
        lines = delta(layout(old_tsv, args.mappings),
                      layout(new_tsv, args.mappings))
        title = new_name if old_name == new_name else "%s -> %s" % (old_name, new_name)
        changes = sum(1 for l in lines if not l.startswith(" "))
        out.append("## %s (%d changes)" % (title, changes))
        out.extend(lines or ["(no change)"])
    text = "\n".join(out) + "\n"
    if args.output:
        with open(args.output, "w") as fh:
            fh.write(text)
    else:
        sys.stdout.write(text)


if __name__ == "__main__":
    main()
