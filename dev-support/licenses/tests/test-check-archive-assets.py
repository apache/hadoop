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
"""Synthetic test suite for check-archive-assets.py (R7 stage C).

Builds a miniature Hadoop-style archive + repo per case and asserts the
gate's verdict. Run: python3 dev-support/licenses/test-check-archive-assets.py
Exit 0 when every case passes.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import zipfile

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GATE = os.path.join(SCRIPT_DIR, "..", "scripts", "check-archive-assets.py")

MANIFEST = """\
apache\troboto-fontface 0.10.0
apache\tleveldb v1.13
mit\tbootstrap v3.4.1
mit\tscope-ui/pkg 1.0.0
mit\thadoop-hdfs-project/hadoop-hdfs/src/main/webapps/static/jquery-3.6.0.min.js
mit\thadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/resources/webapps/static/dt-1.11.5/*
"""

ALLOWLIST = """\
# authored + noise
webapps/static/dfs-dust.js
webapps/static/busy.gif
webapps/*/.keep
*.gz
about.html
css/theme.css
index.html
webapps/ui2/assets/*
WEB-INF/classes/META-INF/LICENSE.txt
"""

UI_TREES = """\
modules/ui-mod webapps/ui2
"""

PIN_EXCEPTIONS = """\
leveldb 1.13\t# native, compiled into a listed jar
"""


def jar(path, files):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with zipfile.ZipFile(path, "w") as z:
        for name, content in files.items():
            z.writestr(name, content)


def build_repo(d, ui_trees=UI_TREES, pin_exceptions=PIN_EXCEPTIONS,
               manifest=MANIFEST, allowlist=ALLOWLIST):
    lic = os.path.join(d, "dev-support", "licenses")
    cur = os.path.join(lic, "curation")
    os.makedirs(cur)
    open(os.path.join(cur, "manifest-assets.txt"), "w").write(manifest)
    open(os.path.join(cur, "archive-asset-allowlist"), "w").write(allowlist)
    open(os.path.join(cur, "ui-archive-trees"), "w").write(ui_trees)
    open(os.path.join(cur, "archive-asset-pin-exceptions"), "w").write(
        pin_exceptions)
    mod = os.path.join(d, "modules", "ui-mod")
    os.makedirs(mod)
    # tier (d): an exact lock pin for scope-ui/pkg@1.0.0
    open(os.path.join(mod, "yarn.lock"), "w").write(
        'scope-ui/pkg@^1.0.0, scope-ui/pkg@1.0.0:\n'
        '  version "1.0.0"\n\n'
        'other@1.0.0:\n  version "9.9.9"\n')
    # tier (d) bower branch: range floor accepted as evidence
    open(os.path.join(mod, "bower.json"), "w").write(
        '{"dependencies":{"bootstrap":"~3.4.1"}}')
    return mod


def build_archive(d, extra_files=None, skip=()):
    root = os.path.join(d, "untarred", "hadoop-3.5.0")
    share = os.path.join(root, "share", "hadoop")
    os.makedirs(share)
    # tier (a): a vendor tree with a real package manifest (catalog-WAR class)
    jar(os.path.join(share, "yarn", "catalog.war"), {
        "vendor/roboto-fontface/package.json":
            '{"name":"roboto-fontface","version":"0.10.0"}',
        "vendor/roboto-fontface/fonts/Roboto.woff": "font",
        "css/theme.css": "authored",
        "index.html": "authored",
        "WEB-INF/classes/META-INF/LICENSE.txt": "meta",
    })
    # path-style coverage via a jar-embedded tree (yarn class); the ui2
    # bundle deliberately carries NO version string so the scope-ui/pkg
    # evidence must come from the lock (tier d)
    jar(os.path.join(share, "yarn", "hadoop-yarn-common-3.5.0.jar"), {
        "webapps/static/dt-1.11.5/js/jquery.dataTables.min.js":
            '/*! DataTables 1.11.5 */',
        "webapps/static/busy.gif": "GIF",
        "webapps/static/dfs-dust.js": "authored",
        "webapps/ui2/assets/root-ABC123.js": "console.log(1)",
        "webapps/ui2/.keep": "",
        "org/apache/hadoop/Foo.class": "",
    })
    # exploded WAR metadata (file-level, the -Pyarn-ui class)
    meta = os.path.join(share, "yarn", "webapps", "ui2",
                        "WEB-INF", "classes", "META-INF")
    os.makedirs(meta)
    open(os.path.join(meta, "LICENSE.txt"), "w").write("meta")
    # file-level webapp assets (hdfs class)
    static = os.path.join(share, "hdfs", "webapps", "static")
    os.makedirs(static)
    open(os.path.join(static, "jquery-3.6.0.min.js"), "w").write("/*! jQuery")
    open(os.path.join(static, "dfs-dust.js"), "w").write("authored")
    open(os.path.join(static, "d3-v4.min.js.gz"), "wb").write(b"gz")
    # a jar carrying a third-party self-description page
    jar(os.path.join(share, "common", "lib", "jetty-9.jar"),
        {"about.html": "<html/>"})
    # native leveldb class: a listed jar with no probeable version
    jar(os.path.join(share, "common", "lib", "leveldbjni-all-1.8.jar"),
        {"META-INF/native/libleveldbjni.so": b"\x7fELF"})
    for path, content in (extra_files or {}).items():
        fp = os.path.join(root, path)
        os.makedirs(os.path.dirname(fp), exist_ok=True)
        if fp.endswith(".jar") or fp.endswith(".war"):
            jar(fp, content)
        else:
            open(fp, "w").write(content)
    for s in skip:
        fp = os.path.join(root, s)
        if os.path.isfile(fp):
            os.remove(fp)
    return os.path.join(d, "untarred")


def run_gate(archive, repo):
    return subprocess.run(
        [sys.executable, GATE, archive, repo,
         "--manifest", os.path.join(repo, "dev-support/licenses/curation/manifest-assets.txt"),
         "--allowlist", os.path.join(repo, "dev-support/licenses/curation/archive-asset-allowlist"),
         "--ui-trees", os.path.join(repo, "dev-support/licenses/curation/ui-archive-trees"),
         "--pin-exceptions", os.path.join(repo, "dev-support/licenses/curation/archive-asset-pin-exceptions")],
        capture_output=True, text=True)


CASES = []


def case(fn):
    CASES.append(fn)
    return fn


@case
def happy_path(d):
    """Every evidence tier fires; the gate passes."""
    build_repo(d)
    a = build_archive(d)
    r = run_gate(a, d)
    assert r.returncode == 0, f"happy path failed:\n{r.stdout}\n{r.stderr}"
    assert "archive asset gate passed" in r.stdout
    assert "probe ok: roboto-fontface 0.10.0 -> shipped" in r.stdout
    assert "scope-ui/pkg 1.0.0 -> modules/ui-mod lock" in r.stdout
    assert "leveldb v1.13 -> pin exception" in r.stdout


@case
def uncovered_asset(d):
    """A shipped third-party asset with no coverage fails (d3 class)."""
    build_repo(d)
    a = build_archive(d, extra_files={
        "share/hadoop/hdfs/webapps/static/unlisted-lib-9.9.9.min.js": "/*! lib"})
    r = run_gate(a, d)
    assert r.returncode == 1 and "UNCOVERED: hdfs/webapps/static/unlisted-lib" in r.stderr.replace(
        "share/hadoop/", ""), r.stderr


@case
def listed_but_absent(d):
    """A manifest path entry with no shipped asset fails (nvd3 class)."""
    build_repo(d)
    a = build_archive(d, skip=["share/hadoop/hdfs/webapps/static/jquery-3.6.0.min.js"])
    r = run_gate(a, d)
    assert r.returncode == 1 and "ABSENT" in r.stderr and "jquery-3.6.0" in r.stderr, r.stderr


@case
def stale_allowlist_row(d):
    """An allowlist row matching nothing fails (self-cleaning)."""
    build_repo(d)
    a = build_archive(d, skip=["share/hadoop/yarn/hadoop-yarn-common-3.5.0.jar"])
    r = run_gate(a, d)
    assert r.returncode == 1 and "STALE ALLOWLIST" in r.stderr, r.stderr


@case
def stale_ui_map_module(d):
    """A ui-archive-trees row whose module dir is gone fails at parse."""
    build_repo(d)
    a = build_archive(d)
    os.makedirs(os.path.join(d, "dev-support/licenses/curation"), exist_ok=True)
    open(os.path.join(d, "dev-support/licenses/curation/ui-archive-trees"), "w").write(
        "modules/gone-mod webapps/ui2\n")
    r = run_gate(a, d)
    assert r.returncode != 0 and "missing modules" in r.stderr, r.stderr


@case
def stale_ui_tree(d):
    """A ui-archive-trees row whose archive tree ships nowhere fails."""
    build_repo(d)
    a = build_archive(d)
    # drop every ui2 asset so the mapped tree ships nowhere (jar entries
    # AND the exploded WAR metadata)
    zpath = os.path.join(a, "hadoop-3.5.0/share/hadoop/yarn/hadoop-yarn-common-3.5.0.jar")
    with zipfile.ZipFile(zpath) as z:
        keep = {n: z.read(n) for n in z.namelist()
                if not n.startswith("webapps/ui2/")}
    jar(zpath, keep)
    shutil.rmtree(os.path.join(a, "hadoop-3.5.0/share/hadoop/yarn/webapps/ui2"))
    r = run_gate(a, d)
    assert r.returncode == 1 and "STALE UI-TREE" in r.stderr, r.stderr


@case
def version_drift(d):
    """A manifest pin the lock disagrees on fails with the shipped hint."""
    build_repo(d)
    a = build_archive(d)
    m = os.path.join(d, "dev-support/licenses/curation/manifest-assets.txt")
    cur = open(m).read()
    open(m, "w").write(cur.replace("scope-ui/pkg 1.0.0", "scope-ui/pkg 0.9.0"))
    r = run_gate(a, d)
    assert r.returncode == 1 and "PROBE MISS" in r.stderr \
        and "scope-ui/pkg 0.9.0" in r.stderr, r.stderr


@case
def scoped_name_normalization(d):
    """@scope/pkg lock keys match manifest names without the '@'."""
    build_repo(d)
    a = build_archive(d)
    # scope-ui/pkg@1.0.0 is pinned in yarn.lock and bundled; pass depends
    # on the normalization of both sides (covered by happy_path too) —
    # here the nested package-lock form is exercised instead
    mod = os.path.join(d, "modules", "ui-mod")
    os.remove(os.path.join(mod, "yarn.lock"))
    jar(os.path.join(mod, "package-lock.json.tar"), {})  # noise
    open(os.path.join(mod, "package-lock.json"), "w").write(
        '{"lockfileVersion":3,"packages":{'
        '"node_modules/@scope-ui/pkg":{"version":"1.0.0"},'
        '"node_modules/parent/node_modules/@scope-ui/pkg":{"version":"0.9.0"}}}')
    r = run_gate(a, d)
    assert r.returncode == 0, f"{r.stdout}\n{r.stderr}"


@case
def stale_pin_exception(d):
    """A pin exception naming no manifest entry fails (self-cleaning)."""
    build_repo(d)
    a = build_archive(d)
    pe = os.path.join(d, "dev-support/licenses/curation/archive-asset-pin-exceptions")
    open(pe, "w").write("ghost-pkg 1.0.0\t# entry pruned, row stayed\n")
    r = run_gate(a, d)
    assert r.returncode != 0 and "STALE PIN-EXCEPTION" in r.stderr, r.stderr


@case
def quoted_multispec_yarn_lock(d):
    """The canonical scoped yarn.lock key form — each spec individually
    quoted, e.g. '"@scope/pkg@^1.0.0", "@scope/pkg@~1.0.0":' — must
    resolve its pin (the old regex silently dropped the whole block)."""
    build_repo(d)
    a = build_archive(d)
    mod = os.path.join(d, "modules", "ui-mod")
    os.remove(os.path.join(mod, "yarn.lock"))
    open(os.path.join(mod, "yarn.lock"), "w").write(
        '# yarn lockfile v1\n\n'
        '"@scope-ui/pkg@^1.0.0", "@scope-ui/pkg@~1.0.0":\n'
        '  version "1.0.0"\n\n'
        'resolved "https://example.org/@scope-ui/pkg"\n')
    r = run_gate(a, d)
    assert r.returncode == 0, f"{r.stdout}\n{r.stderr}"
    assert "scope-ui/pkg 1.0.0 -> modules/ui-mod lock" in r.stdout, r.stdout


@case
def pin_exception_duplicate_name(d):
    """A pin exception is valid when its (name, version) pair matches ANY
    manifest entry of that name — bootstrap/jquery ship at two versions
    from two modules, and the staleness check must not collide on name."""
    build_repo(d)
    a = build_archive(d)
    # give the fixture a second entry for the same name, different version
    m = os.path.join(d, "dev-support/licenses/curation/manifest-assets.txt")
    cur = open(m).read()
    open(m, "w").write(cur + "mit\tscope-ui/pkg 2.0.0\n")
    pe = os.path.join(d, "dev-support/licenses/curation/archive-asset-pin-exceptions")
    cur = open(pe).read()
    open(pe, "w").write(cur + "scope-ui/pkg 2.0.0\t# second listed version\n")
    r = run_gate(a, d)
    # the exception staleness check must validate the (name, version) PAIR:
    # the 1.0.0 row stays valid despite the duplicate name, and the 2.0.0
    # row (matching its own manifest entry) is honored as an exemption too
    assert r.returncode == 0, f"{r.stdout}\n{r.stderr}"
    assert "STALE PIN-EXCEPTION" not in r.stderr, r.stderr
    assert "probe ok: scope-ui/pkg 2.0.0 -> pin exception" in r.stdout, r.stdout


@case
def fail_closed_guards(d):
    """Zero jars, zero assets and a missing manifest must fail, not pass."""
    build_repo(d)
    a = build_archive(d)
    r = run_gate(os.path.join(d, "empty-archive"), d)  # nonexistent root
    assert r.returncode != 0, r.stdout
    empty = os.path.join(d, "no-jars")
    os.makedirs(empty)
    open(os.path.join(empty, "readme.txt"), "w").write("x")
    r = run_gate(empty, d)
    assert r.returncode != 0 and "no jars" in r.stderr, r.stderr
    r = run_gate(a, os.path.join(d, "no-such-repo"))
    assert r.returncode != 0, r.stdout


def main():
    failures = 0
    for fn in CASES:
        d = tempfile.mkdtemp(prefix="asset-gate-test-")
        try:
            fn(d)
            print(f"PASS {fn.__name__}")
        except AssertionError as e:
            failures += 1
            print(f"FAIL {fn.__name__}: {e}")
        finally:
            shutil.rmtree(d, ignore_errors=True)
    if failures:
        sys.exit(f"{failures} case(s) failed")
    print(f"all {len(CASES)} cases passed")


if __name__ == "__main__":
    main()
