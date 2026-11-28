#!/usr/bin/env python3
#
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
"""Archive-side asset gate (requirement 7 stage C).

The archive twin of the jar enumeration (R36): enumerates every non-jar
asset file the assembled distribution ships — BOTH as plain archive files
(hdfs webapps/static, sls html) and EMBEDDED INSIDE Hadoop's own jars
(yarn resources/webapps/static inside hadoop-yarn-common.jar, the
nodemanager TERMINAL/xterm tree, the sls jar's html/) — and matches that
population against the curated manifest-assets.txt, fail-closed.

Checks:
1. Shipped-but-unlisted (the d3-v4.1.1 class): every archive asset must
   be covered by a manifest-assets.txt path-style entry or by the
   archive-asset-allowlist (Hadoop-authored UI code, dist-built .gz
   twins, third-party jars' self-description pages).
2. Listed-but-absent (the nvd3 class, archive side): every archivable
   path-style entry (webapps/html/TERMINAL trees) must match at least
   one shipped asset. Native-source entries (lz4.c, gtest, fuse-dfs...)
   are NOT checked here — they ship compiled into .so files or only in
   the source release; the per-push source-tree gate (stage B) covers
   them.
3. Package-style entries (npm packages): version evidence, most
   anchored first — (a) a shipped vendor|node_modules/<name>/package.json
   or bower.json whose "version" equals the pin; (b) a content probe, the
   pinned "name@version" (or banner/lockfile-style) string in some
   shipped asset; (c) for a package whose installed tree ships but
   carries no version (a lone font file), an EXACT pin (no range) of that
   name in a source-tree package.json; (d) a pin in the shipping module's
   committed LOCK FILE (package-lock.json / yarn.lock / bower.json) while
   the module's built UI tree ships in the archive — the dist build
   installs from those locks, so the pin IS what ships; minified bundles
   carry no probeable version string, the lock is the archive-equivalent
   evidence. bower has no lock file, so a bower range whose floor equals
   the pin is accepted as the strongest available evidence. Entries whose
   content is compiled into a jar with no probeable string (native
   leveldb inside leveldbjni-all) are recorded in
   archive-asset-pin-exceptions — self-cleaning in both directions.
   A miss lists the versions the shipped manifests do carry, so the fix
   is in the message.
   A package-style entry also COVERS (check 1) the files of its own
   installed tree, vendor/<name>/** or node_modules/<name>/** (scoped
   names without the '@'), so npm-installed trees copied into a WAR need
   no path-style entry the source tree cannot resolve.
4. Allowlist self-cleaning: an allowlist entry that matches no shipped
   asset fails, exactly like assembly-includes. The ui-archive-trees map
   self-cleans too: a row whose module dir or archive tree is gone fails.

Coverage matching: a manifest/allowlist entry's path components (after
src/main[.resources] stripping, brace expansion, note stripping) must
appear in the asset's path as a CONTIGUOUS subsequence (fnmatch per
component, so dir entries cover their children and trailing globs work).
This makes file-level paths (share/hadoop/hdfs/webapps/static/d3...) and
embedded jar entries (webapps/static/d3...) match the same curated
source-path entries without any jar->module mapping.

Fail-closed: zero assets, zero jars scanned, a missing/empty manifest or
allowlist, or a malformed line is a failure (exit 2), never a pass.
Failures (exit 1) list every offending entry. An asset TSV inventory is
written for the run artifact of record.

Usage: check-archive-assets.py <untarred-archive-root> [repo-root]
                                 [--manifest manifest-assets.txt]
                                 [--allowlist archive-asset-allowlist]
                                 [--tsv out.tsv]
Defaults: repo root three levels up from this script; manifest and
allowlist from the curation/ sibling directory.
"""

import argparse
import fnmatch
import importlib.util
import json
import os
import re
import sys
import zipfile

sys.dont_write_bytecode = True  # importlib load below must not litter the tree

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# curated inputs live in the curation/ sibling directory
CURATED_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, '..', 'curation'))
DEFAULT_MANIFEST = os.path.join(CURATED_DIR, "manifest-assets.txt")
DEFAULT_ALLOWLIST = os.path.join(CURATED_DIR, "archive-asset-allowlist")
DEFAULT_UI_TREES = os.path.join(CURATED_DIR, "ui-archive-trees")
DEFAULT_PIN_EXCEPTIONS = os.path.join(CURATED_DIR, "archive-asset-pin-exceptions")

# load the shared parsing helpers from the stage-B gate (filename has a
# dash, so import via importlib) — one source of truth for brace
# expansion, note stripping, package-style detection and manifest parsing
_spec = importlib.util.spec_from_file_location(
    "check_asset_manifest", os.path.join(SCRIPT_DIR, "check-asset-manifest.py")
)
_cam = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_cam)
expand_braces = _cam.expand_braces
strip_note = _cam.strip_note
PACKAGE_STYLE = _cam.PACKAGE_STYLE
parse_entries = _cam.parse_entries

# file extensions treated as non-jar assets; plus whole directories that
# carry assets even when individual files have no recognizable extension
# (webapps/<app>/.keep, webapps/api-server/app)
ASSET_EXTS = {
    ".js", ".css", ".html", ".htm", ".map", ".woff", ".woff2", ".ttf",
    ".eot", ".otf", ".svg", ".swf", ".gif", ".png", ".jpg", ".jpeg",
    ".ico", ".gz",
}
ASSET_DIR_HINTS = ("webapps/", "TERMINAL/", "html/")

# java-package-style roots of EMBEDDED entries: resources under these are
# the jar's own internals (code, javadoc, icons) — licensed jar-side by the
# jar's own LICENSE-binary listing, never per-file. Everything else at a
# zip root (vendor/, css/, js/, webapps/, TERMINAL/, html/) is a web asset
# and needs manifest or allowlist coverage. NOTE the deliberate residual
# risk: a third-party jar bundling an asset under a package path would be
# exempt too; that class is indistinguishable from the jar's own resources
# by path alone.
PACKAGE_PATH_ROOTS = ("org/", "com/", "javax/", "java/", "META-INF/")

# directories npm/bower install packages into; the component after one is
# the package name (or @scope, then name)
PKG_DIRS = ("vendor", "node_modules")
PKG_MANIFESTS = ("package.json", "bower.json")
EXACT_PIN = re.compile(r"^\d+\.\d+\.\d+$")

# source-tree path prefixes whose entries ARE archive-checkable; anything
# else (native sources, java sources) ships compiled or not at all
ARCHIVABLE_ROOTS = ("webapps", "html", "TERMINAL")

# package-style probe needles are built from these templates
def probe_needles(name, version):
    base = name.rsplit("/", 1)[-1]
    return [
        f"{name}@{version}",
        f"{base}@{version}",
        f'"{name}": "{version}"',
        f"{name} v{version}",
        f"{name} {version}",
        f"{base} {version}",
        f"{base} v{version}",
    ]


def comps(path):
    return [c for c in path.replace("\\", "/").strip("/").split("/") if c]


def subsequence_match(pat_comps, path_comps):
    """True when pat appears in path as a contiguous component subsequence."""
    n, m = len(pat_comps), len(path_comps)
    if n == 0 or n > m:
        return False
    for i in range(m - n + 1):
        if all(
            fnmatch.fnmatch(path_comps[i + j], pat_comps[j]) for j in range(n)
        ):
            return True
    return False


def manifest_pattern_comps(entry):
    """Path components of one manifest entry, archive-normalized.

    Source paths are rooted at the module dir (…/src/main[/resources]/…);
    archive keys are not (webapps/static/…, TERMINAL/…), so the src/main
    and a leading resources component are stripped. All brace expansions
    are normalized the same way.
    """
    out = []
    for pattern in expand_braces(strip_note(entry.strip())):
        c = comps(pattern)
        for i in range(len(c) - 1):
            if c[i] == "src" and c[i + 1] == "main":
                c = c[i + 2:]
                break
        if c and c[0] == "resources":
            c = c[1:]
        out.append(c)
    return out


def pkg_name(path_comps):
    """npm package name of an installed-tree path (innermost vendor or
    node_modules dir wins, so nested installs resolve), or None."""
    for i in range(len(path_comps) - 3, -1, -1):
        if path_comps[i] in PKG_DIRS:
            nxt = path_comps[i + 1]
            if nxt.startswith("@"):
                if i + 3 >= len(path_comps):
                    return None
                return f"{nxt[1:]}/{path_comps[i + 2]}"
            return nxt
    return None


def is_pkg_manifest(relpath):
    """package.json / bower.json at the ROOT of an installed package."""
    c = comps(relpath)
    if len(c) < 3 or c[-1] not in PKG_MANIFESTS:
        return False
    d = c[:-1]
    return d[-2] in PKG_DIRS or (
        len(d) >= 3 and d[-2].startswith("@") and d[-3] in PKG_DIRS
    )


def source_exact_pins(repo):
    """{npm name: {exact versions}} from the source tree's package.json
    dependency maps (ranges are not evidence and are skipped)."""
    pins = {}
    for dp, dn, fn in os.walk(repo):
        dn[:] = [
            d for d in dn
            if d not in ("node_modules", "target", ".git", ".review")
        ]
        if "package.json" not in fn:
            continue
        try:
            with open(os.path.join(dp, "package.json"), encoding="utf-8") as fh:
                doc = json.load(fh)
        except (OSError, ValueError):
            continue
        for key in ("dependencies", "devDependencies"):
            for name, spec in (doc.get(key) or {}).items():
                if isinstance(spec, str) and EXACT_PIN.match(spec):
                    pins.setdefault(name.lstrip("@"), set()).add(spec)
    return pins


def norm_pkg(name):
    """Scoped npm names arrive in the manifest without the leading '@'
    (radix-ui/react-accordion for @radix-ui/react-accordion); normalize
    both sides by stripping it."""
    return name.lstrip("@")


def lock_exact_pins(module_dir):
    """{npm name: {exact versions}} from a module's committed LOCK FILES —
    the dist build installs from these, so a pin here is what ships.

    package-lock.json: every resolved position (the bundle ships all of
    them — a nested transitive pin is as present in the compiled output
    as the top-level one; the manifest may name either).
    yarn.lock: every resolved block (a name may legitimately resolve to
    two versions for two in-tree consumers).
    bower.json: exact pins, plus the floor of a ~/^ range — bower has no
    lock file, so the floor is the strongest available version evidence.
    Prerelease versions (1.0.0-beta.1) are indexed nowhere: a manifest
    pinning one must carry probe or shipped-manifest evidence instead.
    """
    pins = {}
    for dp, _dn, fn in os.walk(module_dir):
        if "node_modules" in dp.split(os.sep):
            continue
        if "package-lock.json" in fn:
            try:
                doc = json.load(open(os.path.join(dp, "package-lock.json"),
                                     encoding="utf-8"))
            except (OSError, ValueError):
                continue
            pkgs = doc.get("packages") or {}
            if pkgs:  # lockfileVersion 2/3
                for key, info in pkgs.items():
                    if not key.startswith("node_modules/"):
                        continue
                    rest = key[len("node_modules/"):]
                    # nested position: .../node_modules/@scope/pkg — the
                    # package name is the LAST node_modules segment
                    if "node_modules/" in rest:
                        rest = rest.rsplit("node_modules/", 1)[1]
                    v = (info or {}).get("version")
                    if isinstance(v, str) and EXACT_PIN.match(v):
                        pins.setdefault(norm_pkg(rest), set()).add(v)
            else:  # lockfileVersion 1: top-level "dependencies" entries
                for name, info in (doc.get("dependencies") or {}).items():
                    v = (info or {}).get("version")
                    if isinstance(v, str) and EXACT_PIN.match(v):
                        pins.setdefault(norm_pkg(name), set()).add(v)
        if "yarn.lock" in fn:
            try:
                text = open(os.path.join(dp, "yarn.lock"), encoding="utf-8").read()
            except OSError:
                continue
            # key line: 'name@range, name@range2:' — each spec may be
            # individually quoted (the canonical yarn.lock form for scoped
            # packages: '"@scope/pkg@^1.0.0", "@scope/pkg@~1.0.0":'); a
            # name may resolve for several in-tree consumers
            for m in re.finditer(
                r'^([^:\n]+):\n  version "([^"]+)"', text, re.M
            ):
                spec = m.group(1).split(",")[0].strip().strip('"')
                # spec forms: name@range, @scope/name@range; every spec on
                # a key line belongs to the SAME package name (yarn.lock
                # groups one name's resolutions), so the first spec names
                # the block — if that invariant ever changes, versions
                # would be silently dropped here
                if spec.startswith("@"):
                    name = "@" + spec[1:].split("@")[0]
                else:
                    name = spec.split("@")[0]
                if EXACT_PIN.match(m.group(2)):
                    pins.setdefault(norm_pkg(name), set()).add(m.group(2))
        if "bower.json" in fn:
            try:
                doc = json.load(open(os.path.join(dp, "bower.json"),
                                     encoding="utf-8"))
            except (OSError, ValueError):
                continue
            for key in ("dependencies", "devDependencies"):
                for name, spec in (doc.get(key) or {}).items():
                    if not isinstance(spec, str):
                        continue
                    m = re.match(r"^[~^]?v?(\d+\.\d+\.\d+)", spec.strip())
                    if m:  # exact, or the floor of a ~/^ range
                        pins.setdefault(norm_pkg(name), set()).add(m.group(1))
    return pins


def parse_ui_trees(path, repo):
    """[(module_relpath, module_dir, tree_comps)] from ui-archive-trees.
    Self-cleaning: a row whose module dir does not exist in the repo is a
    failure; a row whose tree ships nowhere is caught after enumeration."""
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        sys.exit(f"FAIL: cannot read curated UI-module map ({path})")
    out, failures = [], []
    with open(path, encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, 1):
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) != 2:
                sys.exit(f"FAIL: malformed line {lineno} in {path} "
                         f"(expected <module-path> <archive-tree>): {line!r}")
            module_rel, tree = parts
            module_dir = os.path.join(repo, module_rel)
            if not os.path.isdir(module_dir):
                failures.append(f"line {lineno}: module dir missing: {module_rel}")
                continue
            out.append((module_rel, module_dir, comps(tree)))
    if failures:
        for f in failures:
            print(f"  STALE UI-MAP {f} — drop the row", file=sys.stderr)
        sys.exit("FAIL: ui-archive-trees rows reference missing modules")
    return out


def parse_pin_exceptions(path):
    """{norm_pkg_name: {versions}} exempt from version evidence, from
    archive-asset-pin-exceptions. Self-cleaning: a row with no matching
    manifest entry fails after the manifest is parsed."""
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        sys.exit(f"FAIL: cannot read curated pin exceptions ({path})")
    out = {}
    with open(path, encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, 1):
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            toks = line.split()
            if len(toks) != 2:
                sys.exit(f"FAIL: malformed line {lineno} in {path} "
                         f"(expected <name> <version>): {line!r}")
            out.setdefault(norm_pkg(toks[0]), set()).add(toks[1].lstrip("v"))
    return out


def is_asset(relpath):
    """File-level or jar-entry path that the asset gate must account for."""
    p = relpath.replace("\\", "/")
    if p.endswith("/"):
        return False
    if p.endswith(".class"):
        return False  # compiled code, never a web asset
    ext = os.path.splitext(p)[1].lower()
    return ext in ASSET_EXTS or any(h in p for h in ASSET_DIR_HINTS)


def is_package_internal(entry):
    """Embedded resource under a java package path: the jar's own internals."""
    p = entry.replace("\\", "/")
    return any(p.startswith(root) for root in PACKAGE_PATH_ROOTS)


def enumerate_assets(archive_root):
    """(file_level:set, embedded:set, jars_scanned:int, pkg_json:set).

    file_level keys are relative to the archive's version directory
    (share/hadoop/hdfs/webapps/static/d3...); embedded keys are the raw
    zip entry paths (webapps/static/d3...).
    """
    file_level, embedded, pkg_json = set(), set(), set()
    jars = 0
    for dp, _dn, fn in os.walk(archive_root):
        for name in fn:
            p = os.path.join(dp, name)
            rel = os.path.relpath(p, archive_root)
            top = rel.split(os.sep, 1)
            key = top[1] if len(top) == 2 else rel
            low = name.lower()
            if low.endswith((".jar", ".war")):
                jars += 1
                try:
                    with zipfile.ZipFile(p) as z:
                        for entry in z.namelist():
                            if is_asset(entry):
                                embedded.add(entry)
                            elif is_pkg_manifest(entry):
                                pkg_json.add(entry)
                except zipfile.BadZipFile:
                    print(
                        f"FAIL: unreadable zip in the archive: {rel}",
                        file=sys.stderr,
                    )
                    sys.exit(2)
                continue
            if is_asset(key):
                file_level.add(key)
            elif is_pkg_manifest(key):
                pkg_json.add(key)
    return file_level, embedded, jars, pkg_json


def parse_allowlist(path):
    """[(lineno, comps)] for every non-comment allowlist entry."""
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        sys.exit(f"FAIL: cannot read curated asset allowlist ({path})")
    out = []
    with open(path, encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, 1):
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            out.append((lineno, comps(line)))
    if not out:
        sys.exit(f"FAIL: no entries parsed from allowlist {path}")
    return out


def covered(path_comps, matchers):
    return any(subsequence_match(m, path_comps) for m in matchers)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("archive_root")
    ap.add_argument("repo_root", nargs="?")
    ap.add_argument("--manifest", default=DEFAULT_MANIFEST)
    ap.add_argument("--allowlist", default=DEFAULT_ALLOWLIST)
    ap.add_argument("--ui-trees", default=DEFAULT_UI_TREES)
    ap.add_argument("--pin-exceptions", default=DEFAULT_PIN_EXCEPTIONS)
    ap.add_argument("--tsv", default=None)
    args = ap.parse_args()

    archive_root = args.archive_root
    repo = args.repo_root or os.path.abspath(
        os.path.join(SCRIPT_DIR, os.pardir, os.pardir, os.pardir)
    )
    if not os.path.isdir(archive_root):
        sys.exit(f"FAIL: archive root does not exist: {archive_root}")

    # --- curated inputs (fail-closed) ------------------------------------
    path_entries, package_entries = [], []
    for section, entry in parse_entries(args.manifest):
        if PACKAGE_STYLE.match(entry.strip()):
            package_entries.append((section, entry.strip()))
        else:
            path_entries.append((section, entry.strip()))
    if not path_entries and not package_entries:
        sys.exit(f"FAIL: no entries parsed from {args.manifest} — a "
                 "comments-only or empty manifest is a broken input, not "
                 "a passing gate")
    matchers = []
    archivable = []
    for section, entry in path_entries:
        for c in manifest_pattern_comps(entry):
            matchers.append(c)
            if c and c[0] in ARCHIVABLE_ROOTS:
                archivable.append((section, entry, c))
    allow = parse_allowlist(args.allowlist)
    allow_comps = [c for _ln, c in allow]
    pin_exceptions = parse_pin_exceptions(args.pin_exceptions)
    # every exception row must still name a manifest entry — a stale row
    # means the entry was pruned and the exemption must go with it; a
    # name may legitimately appear twice with different versions (e.g.
    # bootstrap 3.4.1 for one UI, 3.3.7 for the catalog WAR), so the
    # (name, version) PAIR is what must match
    manifest_pkg_index = {}
    for _s, e in package_entries:
        toks = e.split()
        manifest_pkg_index.setdefault(norm_pkg(toks[0]), set()).add(
            toks[-1].lstrip("v"))
    stale_exceptions = [
        (n, v) for n, vs in pin_exceptions.items() for v in vs
        if v not in manifest_pkg_index.get(n, ())
    ]
    if stale_exceptions:
        for n, v in stale_exceptions:
            print(f"  STALE PIN-EXCEPTION: {n} {v} names no manifest entry "
                  "— drop the row", file=sys.stderr)
        sys.exit("FAIL: archive-asset-pin-exceptions rows with no matching "
                 "manifest entry")

    # --- enumerate the archive -------------------------------------------
    file_level, embedded, jars, pkg_json = enumerate_assets(archive_root)
    if jars == 0:
        sys.exit(
            "FAIL: no jars found in the archive — broken archive or wrong "
            "root, refusing to pass vacuously"
        )
    if not file_level and not embedded:
        sys.exit("FAIL: zero asset files enumerated — refusing to pass vacuously")

    all_assets = sorted(file_level) + sorted(embedded)
    if args.tsv:
        os.makedirs(os.path.dirname(os.path.abspath(args.tsv)), exist_ok=True)
        with open(args.tsv, "w", encoding="utf-8") as fh:
            fh.write("kind\tsource\tpath\n")
            for key in sorted(file_level):
                fh.write(f"file\tarchive\t{key}\n")
            for key in sorted(embedded):
                fh.write(f"jar-entry\tarchive\t{key}\n")

    failures = []

    # --- 1. shipped-but-unlisted -----------------------------------------
    # embedded entries under java package paths are the jar's own internals
    # (licensed jar-side); they never need per-file coverage
    internal = {key for key in embedded if is_package_internal(key)}
    package_names = {e.split()[0] for _s, e in package_entries}
    uncovered = [
        key for key in all_assets
        if key not in internal
        and pkg_name(comps(key)) not in package_names
        and not covered(comps(key), matchers)
        and not covered(comps(key), allow_comps)
    ]
    if uncovered:
        failures.append(
            f"{len(uncovered)} shipped asset(s) covered by neither "
            "manifest-assets.txt nor archive-asset-allowlist"
        )
        for key in uncovered:
            print(f"  UNCOVERED: {key}", file=sys.stderr)

    # --- 2. listed-but-absent (archivable entries only) -------------------
    absent = []
    for section, entry, c in archivable:
        if not any(subsequence_match(c, comps(key)) for key in all_assets):
            absent.append((section, entry))
    if absent:
        failures.append(
            f"{len(absent)} archivable manifest entry(ies) match no shipped "
            "asset — prune them or fix the path"
        )
        for section, entry in absent:
            print(f"  ABSENT: [{section}] {entry}", file=sys.stderr)

    # --- 3. package-style content probes ----------------------------------
    # resolve probeable contents in ONE pass: file-level by direct join,
    # embedded by opening each jar once
    def probeable(key):
        return key.lower().endswith(
            (".js", ".css", ".html", ".htm", ".map")
        ) or any(h in key for h in ASSET_DIR_HINTS)

    probe_keys = {key for key in all_assets if probeable(key)} | pkg_json
    blobs = []  # (key, bytes)
    for dp, _dn, fn in os.walk(archive_root):
        for name in fn:
            p = os.path.join(dp, name)
            rel = os.path.relpath(p, archive_root)
            top = rel.split(os.sep, 1)
            fkey = top[1] if len(top) == 2 else rel
            low = name.lower()
            if low.endswith((".jar", ".war")):
                if not probe_keys:
                    break
                try:
                    with zipfile.ZipFile(p) as z:
                        names = set(z.namelist())
                        for key in sorted(probe_keys):
                            if key in names:
                                blobs.append(
                                    (key, z.read(key)[: 8 * 1024 * 1024])
                                )
                except zipfile.BadZipFile:
                    pass  # already hard-failed in enumeration
                continue
            if fkey in probe_keys:
                try:
                    blobs.append((fkey, open(p, "rb").read(8 * 1024 * 1024)))
                except OSError as e:
                    print(f"  WARN: cannot read {rel}: {e}", file=sys.stderr)
    if not blobs:
        sys.exit(
            "FAIL: no probeable asset content found for the package-style "
            "checks — refusing to pass vacuously"
        )
    # (a) versions the shipped package manifests declare, per package name
    shipped_versions = {}
    for key, blob in blobs:
        if key not in pkg_json:
            continue
        try:
            v = json.loads(blob.decode("utf-8", errors="ignore")).get("version")
        except (ValueError, AttributeError):
            continue
        if isinstance(v, str) and v:
            shipped_versions.setdefault(
                norm_pkg(pkg_name(comps(key))), set()
            ).add(v)
    blobs = [(k, b) for k, b in blobs if k not in pkg_json]
    shipped_trees = set()
    for key in all_assets:
        pn = pkg_name(comps(key))
        if pn is not None:
            shipped_trees.add(norm_pkg(pn))
    pins = None  # (c) source exact pins, loaded only when needed

    # (d) lock-file evidence: the UI modules whose built trees ship in
    # this archive; pins are read lazily per module
    ui_modules = parse_ui_trees(args.ui_trees, repo)
    ui_state = [
        (module_rel, tree_c,
         any(subsequence_match(tree_c, comps(key)) for key in all_assets))
        for module_rel, _dir, tree_c in ui_modules
    ]
    lock_cache = {}  # module dir -> pins; one walk per module per run

    def ui_lock_pins(module_dir):
        if module_dir not in lock_cache:
            lock_cache[module_dir] = lock_exact_pins(module_dir)
        return lock_cache[module_dir]

    probe_failures = []
    for section, entry in package_entries:
        toks = entry.split()
        name, version = toks[0], toks[-1]
        bare = version.lstrip("v")
        if pin_exceptions.get(norm_pkg(name), ()) and \
                bare in pin_exceptions[norm_pkg(name)]:
            print(f"  probe ok: {entry} -> pin exception (compiled into a "
                  "listed jar, no probeable string)")
            continue
        if bare in shipped_versions.get(norm_pkg(name), ()):
            print(f"  probe ok: {entry} -> shipped {name}/package manifest")
            continue
        needles = probe_needles(name, version)
        hit = None
        for key, blob in blobs:
            text = blob.decode("utf-8", errors="ignore").lower()
            for needle in needles:
                if needle.lower() in text:
                    hit = (key, needle)
                    break
            if hit:
                break
        if not hit and norm_pkg(name) in shipped_trees and \
                name not in shipped_versions:
            if pins is None:
                pins = source_exact_pins(repo)
            if bare in pins.get(norm_pkg(name), ()):
                hit = ("source package.json", f"exact pin {bare}")
        if not hit:
            # (d) the module's committed lock file pins this exact version
            # AND the module's built UI tree ships in this archive
            for module_rel, tree_c, tree_ships in ui_state:
                if not tree_ships:
                    continue
                module_dir = os.path.join(repo, module_rel)
                lp = ui_lock_pins(module_dir)
                if bare in lp.get(norm_pkg(name), ()):
                    hit = (f"{module_rel} lock", f"lock pin {bare}")
                    break
        if not hit:
            probe_failures.append(
                (section, entry,
                 sorted(shipped_versions.get(norm_pkg(name), ())))
            )
        else:
            print(f"  probe ok: {entry} -> {hit[0]} ({hit[1]!r})")
    if probe_failures:
        failures.append(
            f"{len(probe_failures)} package-style entry(ies) pinned "
            "version string found in NO shipped asset — prune stale "
            "entries or investigate the replacement"
        )
        for section, entry, seen in probe_failures:
            ships = f" (archive ships {', '.join(seen)})" if seen else ""
            print(f"  PROBE MISS: [{section}] {entry}{ships}", file=sys.stderr)

    # --- 4. allowlist self-cleaning ---------------------------------------
    stale_allow = [
        (lineno, "/".join(c))
        for lineno, c in allow
        if not any(subsequence_match(c, comps(key)) for key in all_assets)
    ]
    if stale_allow:
        failures.append(
            f"{len(stale_allow)} allowlist entry(ies) match no shipped "
            "asset — drop the stale rows"
        )
        for lineno, entry in stale_allow:
            print(f"  STALE ALLOWLIST line {lineno}: {entry}", file=sys.stderr)

    # ui-archive-trees self-cleaning: a mapped module whose built tree no
    # longer ships can provide no lock evidence — drop the row
    for module_rel, tree_c, tree_ships in ui_state:
        if not tree_ships:
            failures.append(
                f"ui-archive-trees row {module_rel!r}: archive tree "
                f"{'/'.join(tree_c)!r} ships nowhere — drop the row"
            )
            print(f"  STALE UI-TREE: {module_rel} -> {'/'.join(tree_c)}",
                  file=sys.stderr)

    # --- report -----------------------------------------------------------
    print(
        f"archive assets: {len(file_level)} file-level + {len(embedded)} "
        f"jar-embedded (across {jars} jars/wars) checked against "
        f"{len(path_entries)} path-style + {len(package_entries)} "
        f"package-style manifest entries, {len(allow)} allowlist rows"
    )
    if failures:
        for f in failures:
            print(f"FAIL: {f}", file=sys.stderr)
        sys.exit(1)
    print("archive asset gate passed")


if __name__ == "__main__":
    main()
