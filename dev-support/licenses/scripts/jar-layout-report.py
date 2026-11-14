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
"""Jar layout report for an archive enumeration (pruning input).

Reads one archive-jars.tsv written by enumerate-archive-jars.py and
prints a Markdown report of how the third-party jars are laid out:

1. Version conflicts: one artifact at several versions in the SAME lib
   directory (one classpath entry set, e.g. failureaccess 1.0.2 and
   1.0.3 side by side in share/hadoop/common/lib).
2. Version splits: one artifact at several versions across the archive.
3. Duplicate copies: the same g:a:v in several lib directories, grouped
   by the set of directories that share them (e.g. hdfs/lib repeating
   common/lib), with the bytes the redundant on-disk copies take (all
   but one; WAR-nested copies have no size and are left out) when
   --root names the tree the TSV paths are relative to.

A lib directory is the jar's parent directory; a jar inside a WAR counts
under <war>#WEB-INF/lib. Hadoop's own jars are left out (they are not
pruning candidates for the licence files). Partial rows are identified
through archive-jar-mappings, as the gates do; rows that still resist
are listed, not counted — the archive gate fails on them separately.

Report only: exits 0 on any well-formed input, 2 on malformed input.

Usage: jar-layout-report.py <archive-jars.tsv> [--root <dir>] [--mappings f]
"""

import argparse
import collections
import importlib.util
import os
import sys

sys.dont_write_bytecode = True  # importlib load below must not litter the tree

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# one source of truth for TSV row identification: the R40 generator's
# load_archive_rows (filename has a dash, so import via importlib)
_spec = importlib.util.spec_from_file_location(
    "generate_archive_manifest",
    os.path.join(SCRIPT_DIR, "generate-archive-manifest.py"))
_gam = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_gam)


def lib_dir(jar_path):
    return jar_path.rsplit("/", 1)[0] if "/" in jar_path else "."


def size_of(root, jar_path):
    if root is None or "#" in jar_path:
        return None  # WAR-nested jars have no size on disk
    try:
        return os.path.getsize(os.path.join(root, jar_path))
    except OSError:
        return None


def mib(n):
    return "%.1f MiB" % (n / 1048576.0)


def report(rows, unresolved, root=None):
    out = []
    by_dir_ga = collections.defaultdict(set)   # (dir, g:a) -> versions
    by_ga = collections.defaultdict(set)       # g:a -> versions
    by_gav = collections.defaultdict(list)     # (g:a, v) -> [jar paths]
    for jar_path, ga, version in rows:
        by_dir_ga[(lib_dir(jar_path), ga)].add(version)
        by_ga[ga].add(version)
        by_gav[(ga, version)].append(jar_path)

    dirs = sorted({lib_dir(p) for p, _, _ in rows})
    out.append("# Jar layout report\n")
    out.append("%d third-party jar files, %d distinct g:a:v, %d lib "
               "directories.\n" % (len(rows), len(by_gav), len(dirs)))

    conflicts = sorted(k for k, v in by_dir_ga.items() if len(v) > 1)
    out.append("## 1. Version conflicts in one lib directory (%d)\n"
               % len(conflicts))
    for d, ga in conflicts:
        out.append("- `%s` %s in `%s`"
                   % (ga, ", ".join(sorted(by_dir_ga[(d, ga)])), d))
    out.append("")

    splits = sorted(ga for ga, v in by_ga.items() if len(v) > 1)
    out.append("## 2. Artifacts at several versions across the archive (%d)\n"
               % len(splits))
    for ga in splits:
        where = []
        for v in sorted(by_ga[ga]):
            where.append("%s (%s)" % (v, ", ".join(
                "`%s`" % d for d in sorted({lib_dir(p)
                                            for p in by_gav[(ga, v)]}))))
        out.append("- `%s`: %s" % (ga, "; ".join(where)))
    out.append("")

    groups = collections.defaultdict(list)     # dir set -> [(gav, waste)]
    for (ga, v), paths in by_gav.items():
        pdirs = sorted({lib_dir(p) for p in paths})
        if len(paths) < 2:
            continue
        # on-disk bytes beyond one on-disk copy (WAR-nested copies have
        # no size and are left out); None when no copy has a known size
        sizes = [s for s in (size_of(root, p) for p in paths) if s is not None]
        waste = sum(sizes) - max(sizes) if sizes else None
        groups[tuple(pdirs)].append(("%s:%s" % (ga, v), waste))
    total = sum(len(g) for g in groups.values())
    out.append("## 3. Duplicate copies across lib directories "
               "(%d g:a:v)\n" % total)
    for pdirs, items in sorted(groups.items(), key=lambda kv: -len(kv[1])):
        known = [w for _, w in items if w is not None]
        extra = (", redundant on-disk copies %s" % mib(sum(known))) if known else ""
        out.append("### %d shared by %s%s\n" % (
            len(items), " + ".join("`%s`" % d for d in pdirs), extra))
        for gav, _ in sorted(items):
            out.append("- `%s`" % gav)
        out.append("")

    if unresolved:
        out.append("## Not counted: rows that resist identification (%d)\n"
                   % len(unresolved))
        for p in unresolved:
            out.append("- `%s`" % p)
        out.append("")
    return "\n".join(out)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("tsv", help="archive-jars.tsv")
    parser.add_argument("--root", default=None,
                        help="directory the TSV paths are relative to "
                             "(adds redundant-copy sizes)")
    parser.add_argument("--mappings",
                        default=os.path.join(SCRIPT_DIR, "..", "curation", "archive-jar-mappings"))
    args = parser.parse_args()
    rows, unresolved = _gam.load_archive_rows([args.tsv], args.mappings)
    sys.stdout.write(report(rows, unresolved, args.root) + "\n")


if __name__ == "__main__":
    main()
