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
"""Unused-jar candidates for an assembled distribution (pruning dry run).

Lists the third-party jars an archive ships that nothing in it uses: no
class in any jar reachable from Hadoop's own code refers to them. It is
a DRY RUN: it reads the unpacked archive and writes a report; it changes
no pom, no assembly and no file, and it never fails a build on findings.

How "used" is decided (conservative: when in doubt a jar counts as used,
so a candidate is a jar with no reference at all):

- Nodes are the jars listed in archive-jars.tsv (enumerate-archive-jars.py)
  plus each WAR's WEB-INF/classes. Jars are grouped in units: "main" (every
  jar outside a WAR) and one unit per WAR.
- Roots: Hadoop's own jars (and jars the curation cannot identify, which
  are treated as own), every WEB-INF/classes, and the class names that the
  distribution's etc/, bin/, sbin/ and libexec/ text files mention.
- A node refers to a class when the class's internal or dotted name
  appears in one of the node's class files (constant pool: class entries,
  field/method descriptors and signatures, string constants — which also
  catches Class.forName("...") literals) or text resources (xml,
  properties, json, yaml, conf, MANIFEST.MF; not META-INF/native-image/
  or META-INF/maven/, which name classes without loading them). A
  reference resolves to
  every node of the same unit that holds the class, else to every node
  that holds it anywhere (a WAR's parent class path).
- A node also uses another's resource (a non-class entry outside
  META-INF/) when a string constant names its path, with a directory
  and an extension (getResource("a/b/schema.xsd")): a jar shipped only
  for its schemas or property files counts as used.
- META-INF/services: a provider jar is used when its service interface's
  jar on the same class path (unit) is used, or when the interface is not
  on that class path (a JDK one, or a WAR's parent's) — except
  javax.annotation.processing.Processor, a compile-time-only service.
- Reachability is closed over these references, jar level.

Second section: jars that ARE used, but only by optional features. Each
Hadoop jar belongs to a feature by where it ships (feature()): "core" is
share/hadoop/{common,hdfs,mapreduce,yarn} outside the optional subtrees,
plus what etc/, bin/, sbin/, libexec/ name; a JDK service provider
belongs to the feature where it ships (inside a WAR, the WAR's). The
optional features are each WAR, each tools/lib tool, yarn/timelineservice,
yarn/csi, share/hadoop/client and Hadoop's test jars. A third-party jar
reached from optional features but not from core is listed with the
features that need it: dropping or repackaging those features is what
would remove it. A row whose g:a core also uses is the feature's private
copy (a WAR bundling its own class path): removing it removes the copy,
not the artifact or its LICENSE-binary line.

Not seen: names built at run time (string concatenation, configuration
values set outside the archive), JNI and native loading. Jars with no
class files (native-only, aggregator jars) are listed apart, and
unused-jar-keep lists jars known to be loaded that way; a keep row that
names no shipped jar, or a jar that turns out reachable, is reported as
stale.

Usage: unused-jar-candidates.py <unpacked-dir> <archive-jars.tsv>
           [--keep unused-jar-keep] [--mappings archive-jar-mappings]
           [-o unused-jar-candidates.txt]

<unpacked-dir> is the directory the TSV paths are relative to (for the
dist workflow, target/archive-ground-truth/<name>/untarred).
Exit 0 on any finding; 2 when the inputs are unreadable or malformed.
"""

import argparse
import collections
import importlib.util
import io
import os
import re
import struct
import sys
import zipfile

sys.dont_write_bytecode = True  # importlib load below must not litter the tree

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# the R40 generator's archive row loader: identity of every third-party jar,
# with the archive-jar-mappings promotion the gates use
_spec = importlib.util.spec_from_file_location(
    "generate_archive_manifest",
    os.path.join(SCRIPT_DIR, "generate-archive-manifest.py"))
_gam = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_gam)

TEXT_EXT = (".xml", ".properties", ".json", ".yaml", ".yml", ".conf",
            ".cfg", ".MF", ".sh", ".cmd", ".py")
TEXT_MAX = 2 * 1048576
ROOT_TEXT_DIRS = ("etc", "bin", "sbin", "libexec")
COMPILE_ONLY_SERVICES = {"javax/annotation/processing/Processor"}
# build metadata that names classes without loading them: GraalVM
# reflection configs and embedded poms (shaded jars carry both for every
# artifact they relocate, unrelocated)
METADATA_TREES = ("META-INF/native-image/", "META-INF/maven/")
MR_PREFIX = re.compile(r"^META-INF/versions/\d+/")
DESC = re.compile(r"L([A-Za-z_$][\w$]*(?:/[\w$]+)+)[;<]")
INTERNAL = re.compile(r"[A-Za-z_$][\w$]*(?:/[\w$]+)+")
DOTTED = re.compile(r"[A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)+")
# a resource path in a string constant (getResource("a/b/c.xsd"), optional
# leading slash): at least one directory and an extension, so bare names
# like "log4j.properties" (which many jars carry) do not count
RESOURCE = re.compile(r"/?([\w$-]+(?:/[\w$.-]+)+\.[A-Za-z0-9]+)")


def utf8_constants(data):
    """The CONSTANT_Utf8 strings of one class file (empty if not one)."""
    if len(data) < 10 or data[:4] != b"\xca\xfe\xba\xbe":
        return []
    count = struct.unpack_from(">H", data, 8)[0]
    pos, i, out = 10, 1, []
    fixed = {3: 4, 4: 4, 5: 8, 6: 8, 7: 2, 8: 2, 9: 4, 10: 4, 11: 4,
             12: 4, 15: 3, 16: 2, 17: 4, 18: 4, 19: 2, 20: 2}
    try:
        while i < count:
            tag = data[pos]
            if tag == 1:
                n = struct.unpack_from(">H", data, pos + 1)[0]
                out.append(data[pos + 3:pos + 3 + n].decode("utf-8", "replace"))
                pos += 3 + n
            elif tag in fixed:
                pos += 1 + fixed[tag]
                if tag in (5, 6):
                    i += 1  # long/double take two slots
            else:
                return out  # unknown tag: keep what was read
            i += 1
    except (IndexError, struct.error):
        pass
    return out


def tokens_from_utf8(strings):
    toks = set()
    for s in strings:
        if "/" in s:
            if INTERNAL.fullmatch(s):
                toks.add(s)
            toks.update(DESC.findall(s))
            m = RESOURCE.fullmatch(s)
            if m:
                toks.add(m.group(1))
        if "." in s:
            toks.update(t.replace(".", "/") for t in DOTTED.findall(s))
    return toks


def tokens_from_text(text):
    return {t.replace(".", "/") for t in DOTTED.findall(text)}


class Node:
    __slots__ = ("path", "unit", "classes", "resources", "refs", "services")

    def __init__(self, path, unit):
        self.path, self.unit = path, unit
        self.classes, self.resources, self.refs, self.services = (
            set(), set(), set(), [])


def scan_entries(node, names, read):
    """Fill node from a zip-like listing: class names, refs, services."""
    for name in names:
        if name.endswith("/"):
            continue
        if not name.endswith(".class") and not name.startswith("META-INF/"):
            node.resources.add(name)  # e.g. schemas loaded by getResource
        if name.endswith(".class"):
            cls = MR_PREFIX.sub("", name)[:-6]
            if cls.endswith("module-info"):
                continue
            node.classes.add(cls)
            node.refs |= tokens_from_utf8(utf8_constants(read(name)))
        elif name.startswith("META-INF/services/"):
            iface = name[len("META-INF/services/"):].replace(".", "/")
            impls = [l.split("#")[0].strip() for l in
                     read(name).decode("utf-8", "replace").splitlines()]
            node.services.append((iface, [i for i in impls if i]))
            node.refs |= {i.replace(".", "/") for i in impls if i}
        elif name.endswith(TEXT_EXT) and not name.startswith(METADATA_TREES):
            data = read(name)
            if len(data) <= TEXT_MAX:
                node.refs |= tokens_from_text(data.decode("utf-8", "replace"))


def scan_zip(node, zf, prefix=""):
    names = [n[len(prefix):] for n in zf.namelist() if n.startswith(prefix)]
    scan_entries(node, names, lambda n: zf.read(prefix + n))


def load_nodes(root, tsv):
    """One Node per TSV row, plus WEB-INF/classes of every WAR."""
    paths = []
    for line in _gam.open_or_fail(tsv, "archive TSV"):
        paths.append(line.split("\t", 1)[0])
    nodes, wars, cache = [], {}, {}

    def war(path):
        if path not in wars:
            try:
                wars[path] = zipfile.ZipFile(os.path.join(root, path))
            except (OSError, zipfile.BadZipFile) as err:
                _gam.fail("cannot open %s: %s" % (path, err))
        return wars[path]

    for path in paths:
        outer, _, inner = path.partition("#")
        node = Node(path, outer if inner else "main")
        try:
            if inner:
                data = war(outer).read(inner)
            else:
                with open(os.path.join(root, path), "rb") as fh:
                    data = fh.read()
        except (OSError, KeyError) as err:
            _gam.fail("cannot read %s under %s: %s" % (path, root, err))
        # copies of one jar (hdfs/lib repeats common/lib) are scanned once
        key = (path.rsplit("/", 1)[-1], len(data))
        if key not in cache:
            try:
                scan_zip(node, zipfile.ZipFile(io.BytesIO(data)))
            except zipfile.BadZipFile as err:
                _gam.fail("%s is not a jar: %s" % (path, err))
            cache[key] = node
        else:
            first = cache[key]
            node.classes, node.resources, node.refs, node.services = (
                first.classes, first.resources, first.refs, first.services)
        nodes.append(node)
    for w, zf in wars.items():
        node = Node(w + "#WEB-INF/classes", w)
        scan_zip(node, zf, "WEB-INF/classes/")
        for extra in ("WEB-INF/web.xml",):
            if extra in zf.namelist():
                node.refs |= tokens_from_text(
                    zf.read(extra).decode("utf-8", "replace"))
        nodes.append(node)
    return nodes


def root_text_tokens(root):
    toks = set()
    for top in os.listdir(root):
        base = os.path.join(root, top)
        if not os.path.isdir(base):
            continue
        for sub in ROOT_TEXT_DIRS:
            start = os.path.join(base, sub)
            for dirpath, _dirs, files in os.walk(start):
                for f in files:
                    p = os.path.join(dirpath, f)
                    try:
                        if os.path.getsize(p) > TEXT_MAX:
                            continue
                        with open(p, "rb") as fh:
                            toks |= tokens_from_text(
                                fh.read().decode("utf-8", "replace"))
                    except OSError:
                        continue
    return toks


def jar_graph(nodes):
    """Jar-level edges: node -> nodes it refers to (same unit first, else
    anywhere), plus service-interface jar -> provider jars. Also the
    class/resource index and the providers of JDK interfaces."""
    index = collections.defaultdict(list)
    for i, n in enumerate(nodes):
        for c in n.classes:
            index[c].append(i)
        for r in n.resources:
            index[r].append(i)

    def resolve(tok, unit):
        owners = index.get(tok)
        if not owners:
            return ()
        same = [o for o in owners if nodes[o].unit == unit]
        return same or owners

    adj = [set() for _ in nodes]
    jdk_providers = set()
    for i, n in enumerate(nodes):
        for tok in n.refs:
            adj[i].update(resolve(tok, n.unit))
        for iface, _impls in n.services:
            if iface in COMPILE_ONLY_SERVICES:
                continue
            # a provider is found by the class loader of its own class
            # path: only the interface's jars there pull it in; an interface
            # from elsewhere (the JDK, a parent class path) makes it a seed
            same = [o for o in index.get(iface, ()) if nodes[o].unit == n.unit]
            if same:
                for o in same:
                    adj[o].add(i)  # the interface's jar pulls the provider
            else:
                jdk_providers.add(i)
    return adj, index, resolve, jdk_providers


def closure(adj, seeds):
    seen = set(seeds)
    queue = list(seen)
    while queue:
        for j in adj[queue.pop()]:
            if j not in seen:
                seen.add(j)
                queue.append(j)
    return seen


def reachable(nodes, roots, root_tokens):
    """Indices of the nodes reachable from roots (see module doc)."""
    adj, index, resolve, jdk_providers = jar_graph(nodes)
    seeds = set(roots) | jdk_providers
    for tok in root_tokens:
        seeds.update(resolve(tok, "main"))
    return closure(adj, seeds), index


TEST_JAR = re.compile(r"-tests?\.jar$|-test-sources\.jar$")
VERSIONED = re.compile(r"-\d[\w.\-]*(\.(?:jar|war))$")


def feature(path):
    """The feature a Hadoop jar (or WEB-INF/classes) belongs to, from where
    it ships: "core" is what the daemons and the hadoop command put on the
    class path (share/hadoop/{common,hdfs,mapreduce,yarn} outside the
    optional subtrees); everything else is optional."""
    outer, _, inner = path.partition("#")
    base = outer.rsplit("/", 1)[-1]
    if inner:
        return VERSIONED.sub(r"\1", base)  # the WAR
    if TEST_JAR.search(base):
        return "tests"
    for marker, name in (("/share/hadoop/client/", "client"),
                         ("/share/hadoop/yarn/timelineservice/",
                          "yarn-timelineservice"),
                         ("/share/hadoop/yarn/csi/", "yarn-csi")):
        if marker in path:
            return name
    if "/share/hadoop/tools/" in path:
        return "tools:" + VERSIONED.sub("", base)
    return "core"


def placement(path):
    """The feature whose class path a (third-party) jar is on: a JDK
    service provider is loaded only when that class path is in use."""
    f = feature(path)
    if f.startswith("tools:"):
        return "tools/lib"
    return "core" if f == "tests" else f


def optional_only(nodes, roots, root_tokens, third):
    """Third-party nodes reached, but not from core: node -> features."""
    adj, _index, resolve, jdk_providers = jar_graph(nodes)
    by_feature = collections.defaultdict(set)
    for i in roots:
        by_feature[feature(nodes[i].path)].add(i)
    # JDK service providers belong to the feature where they ship (one
    # inside a WAR is loaded only with that WAR); what the scripts name is core
    for i in jdk_providers:
        by_feature[placement(nodes[i].path)].add(i)
    for tok in root_tokens:
        by_feature["core"].update(resolve(tok, "main"))
    core = closure(adj, by_feature.pop("core", ()))
    out = collections.defaultdict(set)
    for name, seeds in by_feature.items():
        for j in closure(adj, seeds):
            if j not in core and nodes[j].path in third:
                out[j].add(name)
    core_gas = {third[nodes[i].path][0] for i in core if nodes[i].path in third}
    return out, core_gas


def load_keep(path):
    keep = {}
    if not path or not os.path.exists(path):
        return keep
    for line in _gam.open_or_fail(path, "unused-jar-keep"):
        parts = line.split("#", 1)
        ga = parts[0].strip().lower()
        if ga:
            keep[ga] = parts[1].strip() if len(parts) > 1 else ""
    return keep


def report(nodes, third, seen, index, keep, roots, optional=None,
           core_gas=frozenset()):
    """Text report: candidates, no-class jars, keep-listed, stale keep."""
    out = []
    candidates, noclass, kept = [], [], []
    for i, n in enumerate(nodes):
        if i in seen or n.path not in third:
            continue
        ga, v = third[n.path]
        entry = (ga, v, n)
        if not n.classes:
            noclass.append(entry)
        elif ga in keep:
            kept.append(entry)
        else:
            candidates.append(entry)
    shipped = {ga for ga, _ in third.values()}
    reached = {third[nodes[i].path][0] for i in seen if nodes[i].path in third}
    stale = sorted(ga for ga in keep if ga not in shipped or ga in reached)

    units = collections.Counter(n.unit for n in nodes)
    out.append("# Unused-jar candidates (dry run: nothing is changed)")
    out.append("# %d nodes in %d units, %d roots, %d reached; %d third-party "
               "jar files" % (len(nodes), len(units), len(roots), len(seen),
                              len(third)))
    out.append("# columns: g:a:v <TAB> path <TAB> note")
    out.append("")

    def dup_note(n):
        others = set()
        for c in n.classes:
            others.update(o for o in index[c] if nodes[o] is not n
                          and nodes[o].classes is not n.classes)
        full = [o for o in others if n.classes <= nodes[o].classes]
        return ("all classes also in " + nodes[full[0]].path) if full else ""

    out.append("## candidates: shipped, never referenced (%d)" % len(candidates))
    for ga, v, n in sorted(candidates, key=lambda e: (e[0], e[2].path)):
        out.append("%s:%s\t%s\t%s" % (ga, v, n.path, dup_note(n)))
    out.append("")
    if optional is not None:
        groups = collections.defaultdict(list)
        for j, feats in optional.items():
            groups[" + ".join(sorted(feats))].append(nodes[j])
        gas = {third[nodes[j].path][0] for j in optional}
        out.append("## used only by optional features, not by core (%d jar "
                   "files; %d g:a that only these features need, %d private "
                   "copies of g:a core uses too)" % (
                       len(optional), len(gas - core_gas),
                       sum(1 for j in optional
                           if third[nodes[j].path][0] in core_gas)))
        for feats, ns in sorted(groups.items(), key=lambda kv: (-len(kv[1]), kv[0])):
            out.append("### %s (%d)" % (feats, len(ns)))
            for n in sorted(ns, key=lambda n: (third[n.path][0], n.path)):
                ga, v = third[n.path]
                note = feats + ("; copy: core uses this g:a too"
                                if ga in core_gas else "")
                out.append("%s:%s\t%s\t%s" % (ga, v, n.path, note))
        out.append("")
    out.append("## no class files: native or aggregator jars, loaded by name "
               "or not at all — review by hand (%d)" % len(noclass))
    for ga, v, n in sorted(noclass, key=lambda e: (e[0], e[2].path)):
        out.append("%s:%s\t%s\t" % (ga, v, n.path))
    out.append("")
    out.append("## keep-listed: never referenced, kept by unused-jar-keep (%d)"
               % len(kept))
    for ga, v, n in sorted(kept, key=lambda e: (e[0], e[2].path)):
        out.append("%s:%s\t%s\t%s" % (ga, v, n.path, keep[ga]))
    out.append("")
    out.append("## stale keep rows: not shipped, or referenced anyway (%d)"
               % len(stale))
    out.extend(stale)
    return "\n".join(out) + "\n"


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("root", help="directory the TSV paths are relative to")
    parser.add_argument("tsv", help="archive-jars.tsv")
    parser.add_argument("--keep", default=os.path.join(SCRIPT_DIR, "..", "curation", "unused-jar-keep"))
    parser.add_argument("--mappings",
                        default=os.path.join(SCRIPT_DIR, "..", "curation", "archive-jar-mappings"))
    parser.add_argument("-o", "--output", default=None)
    args = parser.parse_args()

    rows, _unresolved = _gam.load_archive_rows([args.tsv], args.mappings)
    third = {path: (ga, v) for path, ga, v in rows}
    nodes = load_nodes(args.root, args.tsv)
    # own jars and jars the curation cannot identify are roots, as is
    # every WEB-INF/classes
    # ponytail: own *-tests jars are roots too, so test-only deps they
    # reference count as used; drop tests jars from the roots if that hides
    # real candidates
    roots = {i for i, n in enumerate(nodes) if n.path not in third}
    root_tokens = root_text_tokens(args.root)
    seen, index = reachable(nodes, roots, root_tokens)
    optional, core_gas = optional_only(nodes, roots, root_tokens, third)
    text = report(nodes, third, seen, index, load_keep(args.keep), roots,
                  optional, core_gas)
    if args.output:
        with open(args.output, "w") as fh:
            fh.write(text)
    else:
        sys.stdout.write(text)


if __name__ == "__main__":
    main()
