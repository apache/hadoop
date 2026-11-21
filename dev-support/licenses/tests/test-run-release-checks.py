#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for run-release-checks.sh (R44, the release-time driver).

Builds a fully synthetic, internally consistent release world in a temp
dir — fake repo checkout (curation files, texts store, committed
LICENSE-binary/NOTICE-binary), third-party inventory, and two binary
distribution tarballs built from real (tiny) jar files — then runs the
driver end to end: green release mode, green verification mode, and
every fail-closed path (each must exit 1 and print the fix-list entry
for the right requirement). Run directly:

    python3 dev-support/licenses/test-run-release-checks.py
"""

import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))
DRIVER = os.path.join(HERE, "..", "run-release-checks.sh")
GEN = os.path.join(HERE, "..", "scripts", "generate-archive-manifest.py")

INVENTORY = (
    "     (The Apache Software License, Version 2.0) Gson"
    " (com.google.code.gson:gson:2.9.0 - https://example.org/gson)\n"
    "     (MIT License) MIT Lib (org.example:mitlib:1.0 - https://example.org/mitlib)\n"
)

MIT_TEXT = (
    "MIT License\n\nCopyright (c) 2026 The MIT Lib Authors\n\n"
    "Permission is hereby granted, free of charge, to any person "
    "obtaining a copy of this software and associated documentation "
    "files (the \"Software\"), to deal in the Software without "
    "restriction.\n"
)


def make_jar(path, group, artifact, version, with_metadata=True):
    """A tiny real jar; enumeration resolves g:a:v from pom.properties."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("META-INF/MANIFEST.MF",
                    "Manifest-Version: 1.0\nCreated-By: test\n\n")
        if with_metadata:
            props = ("groupId=%s\nartifactId=%s\nversion=%s\n"
                     % (group, artifact, version))
            zf.writestr("META-INF/maven/%s/%s/pom.properties"
                        % (group, artifact), props)
            zf.writestr("META-INF/maven/%s/%s/pom.xml" % (group, artifact),
                        "<project/>")
        zf.writestr("org/example/Clazz.class", b"\xca\xfe\xba\xbe\x00\x00")


def run_generator(out, tsvs, repo):
    """Produce the committed LICENSE-binary the same way the driver will,
    so the drift gate starts in sync."""
    cmd = [sys.executable, GEN,
           "--inventory", repo["inventory"],
           "--mappings", repo["dl"] + "/curation/archive-jar-mappings",
           "--shaded", repo["dl"] + "/curation/shaded-jars",
           "--assets", repo["dl"] + "/curation/manifest-assets.txt",
           "--template", repo["dl"] + "/curation/manifest-template.txt",
           "--store", repo["dl"] + "/texts",
           "--aliases", repo["dl"] + "/curation/licenses-binary-aliases",
           "--generic", repo["dl"] + "/curation/texts-generic",
           "--archive-tsv"] + tsvs + ["--out", out]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    return open(out).read()


def build_world(tmp):
    """Create (a) the fake repo checkout, (b) the tarball build directory,
    (c) the enumeration TSVs of the final tarballs (needed to produce the
    committed manifest before the tarballs exist: the manifest rides
    inside them). Returns a dict of paths."""
    root = os.path.join(tmp, "repo")
    dl = os.path.join(root, "dev-support", "licenses")
    texts = os.path.join(dl, "texts")
    os.makedirs(texts)
    # the driver validates the scripts/ and curation/ subdirs of the
    # checkout it gates
    os.makedirs(os.path.join(dl, "scripts"), exist_ok=True)
    os.makedirs(os.path.join(dl, "curation"), exist_ok=True)
    store = {}

    def put(rel, content):
        path = os.path.join(root, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as fh:
            fh.write(content)
        return path

    store["inventory"] = put("third-party/THIRD-PARTY.txt", INVENTORY)
    # a UI module dir the ui-archive-trees row maps (self-cleaning: the
    # module dir must exist in the repo)
    os.makedirs(os.path.join(root, "ui-mod"), exist_ok=True)
    # curation files: consistent, minimal
    for name, content in (
        ("archive-jar-mappings", ""),
        ("assembly-includes", ""),
        ("shaded-jars", ""),
        # one curated asset entry + one allowlist row + the matching
        # asset file in the tarball: the R7 asset gate refuses empty
        # configs and empty archives by design
        ("manifest-assets.txt", "apache\tlib/asset.js\napache\tsome-npm 1.0\n"),
        # allowlist + UI-map rows are self-cleaning: both must match
        # something that ships (the asset file / the lib tree)
        ("archive-asset-allowlist", "lib/asset.js\n"),
        # the package-style entry's version is not probeable in the tiny
        # fixture archive: exempt it exactly as a real pin row does
        ("archive-asset-pin-exceptions", "some-npm 1.0\n"),
        ("ui-archive-trees", "ui-mod lib\n"),
        ("notice-aliases", ""),
        ("notice-exceptions", ""),
        ("licenses-binary-aliases", ""),
        ("texts-generic", ""),
        ("manifest-template.txt", "template preamble\n"),
    ):
        put("dev-support/licenses/curation/" + name, content)
    put("dev-support/licenses/texts/LICENSE-mit.txt", MIT_TEXT)
    # the committed NOTICE-binary covers every attribution-required
    # component (mitlib; gson rides the untitled Apache section)
    put("NOTICE-binary",
        "Apache Hadoop\nCopyright 2026 The Apache Software Foundation\n\n"
        "This product includes mitlib developed at Example.org.\n")
    # the committed manifest of record is filled in below, after the
    # generator produced it (build_world returns before that today) —
    # written by the caller via finish_world()
    put("LICENSE-binary", "")

    # tarball payloads
    build = os.path.join(tmp, "build")
    for top in ("hadoop-3.6.0", "hadoop-cloud-storage-dist-3.6.0"):
        os.makedirs(os.path.join(build, top, "lib"), exist_ok=True)
    main_top = os.path.join(build, "hadoop-3.6.0")
    cloud_top = os.path.join(build, "hadoop-cloud-storage-dist-3.6.0")
    make_jar(os.path.join(main_top, "lib", "gson-2.9.0.jar"),
             "com.google.code.gson", "gson", "2.9.0")
    make_jar(os.path.join(main_top, "lib", "mitlib-1.0.jar"),
             "org.example", "mitlib", "1.0")
    with open(os.path.join(main_top, "lib", "asset.js"), "w") as fh:
        fh.write("// curated asset\n")
    make_jar(os.path.join(cloud_top, "lib", "gson-2.9.0.jar"),
             "com.google.code.gson", "gson", "2.9.0")

    # TSVs as enumerate-archive-jars.py would produce them (the manifest
    # is generated from the FINAL archives, then packed inside them)
    tsv_main = os.path.join(tmp, "tsv-main")
    tsv_cloud = os.path.join(tmp, "tsv-cloud")
    rows = ("lib/%s-%s.jar\t%s\t%s\t%s\tpom.properties\n")
    with open(tsv_main, "w") as fh:
        fh.write(rows % ("gson", "2.9.0", "com.google.code.gson", "gson", "2.9.0"))
        fh.write(rows % ("mitlib", "1.0", "org.example", "mitlib", "1.0"))
    with open(tsv_cloud, "w") as fh:
        fh.write(rows % ("gson", "2.9.0", "com.google.code.gson", "gson", "2.9.0"))

    manifest = run_generator(os.path.join(tmp, "committed-LICENSE-binary"),
                             [tsv_main, tsv_cloud],
                             {"inventory": store["inventory"], "dl": dl})
    licenses_dir = os.path.join(tmp, "committed-licenses-binary")
    subprocess.run([sys.executable, GEN, "--emit-licenses-dir-only",
                    "--manifest", os.path.join(tmp, "committed-LICENSE-binary"),
                    "--store", texts,
                    "--aliases", dl + "/curation/licenses-binary-aliases",
                    "--generic", dl + "/curation/texts-generic",
                    "--licenses-dir", licenses_dir],
                   check=True, capture_output=True)
    shipped = os.listdir(licenses_dir)
    assert shipped, "emitter shipped no texts — fixture is broken"

    # pack the tarballs: manifest + texts dir ride inside each archive
    def pack(top, extra_license):
        with open(os.path.join(top, "LICENSE-binary"), "w") as fh:
            fh.write(manifest)
        dest = os.path.join(top, "licenses-binary")
        os.makedirs(dest, exist_ok=True)
        for name in shipped:
            shutil.copyfile(os.path.join(licenses_dir, name),
                            os.path.join(dest, name))
        out = top + ".tar.gz"
        with tarfile.open(out, "w:gz") as tf:
            tf.add(top, arcname=os.path.basename(top))
        return out

    main_tar = pack(main_top, None)
    cloud_tar = pack(cloud_top, None)
    # a source archive: present in the release dir, never jar-gated
    src_tar = os.path.join(build, "hadoop-3.6.0-src.tar.gz")
    with tarfile.open(src_tar, "w:gz") as tf:
        tf.add(os.path.join(build, "hadoop-3.6.0", "lib"),
               arcname="hadoop-3.6.0-src/pom")

    return {"root": root, "dl": dl, "inventory": store["inventory"],
            "build": build, "main": main_tar, "cloud": cloud_tar,
            "src": src_tar, "manifest": manifest,
            "licenses_dir": licenses_dir}


def run_driver(args):
    return subprocess.run(["bash", DRIVER] + args,
                          capture_output=True, text=True)


def expect(cond, msg, proc=None):
    if not cond:
        detail = ""
        if proc is not None:
            detail = "\n--- stdout ---\n%s\n--- stderr ---\n%s" % (
                proc.stdout[-3000:], proc.stderr[-3000:])
        raise AssertionError(msg + detail)


def fresh_world():
    tmp = tempfile.mkdtemp(prefix="rrc-test-")
    w = build_world(tmp)
    # the committed LICENSE-binary is exactly what the generator produced
    # from the final archives' TSVs — the R40 drift gate starts in sync
    with open(os.path.join(w["root"], "LICENSE-binary"), "w") as fh:
        fh.write(w["manifest"])
    return tmp, w


def case_green_release():
    tmp, w = fresh_world()
    try:
        out = os.path.join(tmp, "out")
        proc = run_driver(["--tarballs", w["build"], "--repo", w["root"],
                           "--third-party", w["inventory"], "--out", out])
        expect(proc.returncode == 0, "release mode must be green", proc)
        expect("RELEASE LICENSE CHECKS: GREEN" in proc.stdout,
               "green banner missing", proc)
        bundle = os.path.join(out, "rc-bundle")
        for name in ("THIRD-PARTY.txt", "LICENSE-binary.generated",
                     "licenses-binary", "SHA512SUMS", "CHECKLIST.md"):
            expect(os.path.exists(os.path.join(bundle, name)),
                   "bundle missing %s" % name, proc)
        # the generated manifest matches what the fixture committed
        expect(open(os.path.join(bundle, "LICENSE-binary.generated")).read()
               == w["manifest"], "bundle manifest != committed", proc)
        # the source archive was noted, not gated
        expect("hadoop-3.6.0-src.tar.gz" in proc.stdout,
               "source archive not noted", proc)
        # checksums verify
        sums = os.path.join(bundle, "SHA512SUMS")
        chk = subprocess.run(["sha512sum", "-c", sums], cwd=w["build"],
                             capture_output=True, text=True)
        expect(chk.returncode == 0,
               "SHA512SUMS does not verify: " + chk.stdout, proc)
    finally:
        shutil.rmtree(tmp)


def case_two_tarball_flags():
    """Regression (first real CI run, 43297145629): the workflow passes
    TWO distribution target directories after ONE --tarballs flag; the
    parser treated the second as an unknown argument."""
    tmp, w = fresh_world()
    try:
        out = os.path.join(tmp, "out")
        proc = run_driver(["--tarballs", w["main"], w["cloud"],
                           "--repo", w["root"],
                           "--third-party", w["inventory"], "--out", out])
        expect(proc.returncode == 0,
               "two values after one --tarballs flag must gate both", proc)
        expect("hadoop-3.6.0" in proc.stdout
               and "hadoop-cloud-storage-dist-3.6.0" in proc.stdout,
               "both archives must be gated", proc)
    finally:
        shutil.rmtree(tmp)


def case_green_verify():
    tmp, w = fresh_world()
    try:
        rc = os.path.join(tmp, "rc")
        os.makedirs(rc)
        for tar in (w["main"], w["cloud"]):
            shutil.copyfile(tar, os.path.join(rc, os.path.basename(tar)))
            sums = subprocess.run(["sha512sum", os.path.basename(tar)],
                                  cwd=w["build"], capture_output=True, text=True)
            with open(os.path.join(rc, os.path.basename(tar)) + ".sha512",
                      "w") as fh:
                fh.write(sums.stdout)
        shutil.copyfile(w["inventory"], os.path.join(rc, "THIRD-PARTY.txt"))
        proc = run_driver(["--verify-only", rc, "--repo", w["root"]])
        expect(proc.returncode == 0, "verification mode must be green", proc)
        expect("RELEASE LICENSE CHECKS: GREEN" in proc.stdout,
               "green banner missing", proc)
        expect("sha512 OK" in proc.stdout, "checksum result missing", proc)
        # NOTICE coverage is a committed-state gate: verify mode notes its
        # scope explicitly
        expect("NOTICE and committed-state gates" in proc.stdout
               and "release mode" in proc.stdout,
               "NOTICE scope note missing", proc)
    finally:
        shutil.rmtree(tmp)


def case_r38():
    """The shipped LICENSE-binary does not state an shipped jar's license."""
    tmp, w = fresh_world()
    try:
        # committed manifest WITHOUT mitlib, packed with both jars:
        # the shipped manifest cannot cover mitlib
        top = os.path.join(w["build"], "hadoop-3.6.0")
        lb = os.path.join(top, "LICENSE-binary")
        lines = open(lb).read()
        lines = lines.replace("org.example:mitlib:1.0", "")
        with open(lb, "w") as fh:
            fh.write(lines)
        tar = top + ".tar.gz"
        with tarfile.open(tar, "w:gz") as tf:
            tf.add(top, arcname="hadoop-3.6.0")
        out = os.path.join(tmp, "out")
        proc = run_driver(["--tarballs", tar, "--repo", w["root"],
                           "--third-party", w["inventory"], "--out", out])
        expect(proc.returncode == 1, "R38 failure must be red", proc)
        expect("[coverage]" in proc.stdout, "fix list lacks R38", proc)
        expect("regenerate LICENSE-binary" in proc.stdout,
               "R38 fix line lacks guidance", proc)
    finally:
        shutil.rmtree(tmp)


def case_r36_unnameable():
    """A jar whose coordinates come only from its manifest (no
    pom.properties) enumerates as a partial row: R36 fails until a
    curated mapping names it. (A jar with NO metadata at all dies
    earlier, at enumeration.)"""
    tmp, w = fresh_world()
    try:
        top = os.path.join(w["build"], "hadoop-3.6.0")
        path = os.path.join(top, "lib", "mystery-1.0.jar")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with zipfile.ZipFile(path, "w") as zf:
            zf.writestr("META-INF/MANIFEST.MF",
                        "Manifest-Version: 1.0\n\n"
                        "Implementation-Title: mystery\n"
                        "Implementation-Version: 1.0\n\n")
        tar = top + ".tar.gz"
        with tarfile.open(tar, "w:gz") as tf:
            tf.add(top, arcname="hadoop-3.6.0")
        out = os.path.join(tmp, "out")
        proc = run_driver(["--tarballs", tar, "--repo", w["root"],
                           "--third-party", w["inventory"], "--out", out])
        expect(proc.returncode == 1, "partially nameable jar must be red", proc)
        expect("[jar-naming]" in proc.stdout, "fix list lacks R36", proc)
        expect("archive-jar-mappings" in proc.stdout,
               "R36 fix must name archive-jar-mappings", proc)
    finally:
        shutil.rmtree(tmp)


def case_r40_drift():
    tmp, w = fresh_world()
    try:
        # the committed manifest of record drifts from what the archives
        # contain: the R10/R40 gate must go red and point at the
        # regenerated file
        with open(os.path.join(w["root"], "LICENSE-binary"), "a") as fh:
            fh.write("hand-edited line that does not belong\n")
        out = os.path.join(tmp, "out")
        proc = run_driver(["--tarballs", w["build"], "--repo", w["root"],
                           "--third-party", w["inventory"], "--out", out])
        expect(proc.returncode == 1, "committed drift must be red", proc)
        expect("[drift]" in proc.stdout, "fix list lacks R40", proc)
        expect("generated-LICENSE-binary" in proc.stdout,
               "R40 fix must point at the generated file", proc)
    finally:
        shutil.rmtree(tmp)


def case_r53_drift():
    tmp, w = fresh_world()
    try:
        top = os.path.join(w["build"], "hadoop-3.6.0")
        with open(os.path.join(top, "licenses-binary", "LICENSE-mit.txt"),
                  "a") as fh:
            fh.write("tampered\n")
        tar = top + ".tar.gz"
        with tarfile.open(tar, "w:gz") as tf:
            tf.add(top, arcname="hadoop-3.6.0")
        out = os.path.join(tmp, "out")
        proc = run_driver(["--tarballs", tar, "--repo", w["root"],
                           "--third-party", w["inventory"], "--out", out])
        expect(proc.returncode == 1, "shipped-texts drift must be red", proc)
        expect("[texts]" in proc.stdout, "fix list lacks R53", proc)
    finally:
        shutil.rmtree(tmp)


def case_r14_uncovered():
    tmp, w = fresh_world()
    try:
        notice = os.path.join(w["root"], "NOTICE-binary")
        with open(notice, "w") as fh:
            fh.write("Apache Hadoop\nCopyright 2026 The Apache Software"
                     " Foundation\n")
        out = os.path.join(tmp, "out")
        proc = run_driver(["--tarballs", w["build"], "--repo", w["root"],
                           "--third-party", w["inventory"], "--out", out])
        expect(proc.returncode == 1, "uncovered NOTICE component must be red", proc)
        expect("[notice]" in proc.stdout, "fix list lacks R14", proc)
        expect("notice-exceptions" in proc.stdout,
               "R14 fix must name the notice configuration", proc)
    finally:
        shutil.rmtree(tmp)


def case_enum_failure():
    tmp, w = fresh_world()
    try:
        bad = os.path.join(tmp, "broken.tar.gz")
        with open(bad, "wb") as fh:
            fh.write(b"this is not gzip\n")
        out = os.path.join(tmp, "out")
        proc = run_driver(["--tarballs", bad, "--repo", w["root"],
                           "--third-party", w["inventory"], "--out", out])
        expect(proc.returncode == 1, "unenumerable archive must be red", proc)
        expect("[enumeration]" in proc.stdout, "fix list lacks ENUM", proc)
        # no generation may have run on zero TSVs
        expect(not os.path.exists(os.path.join(out, "generated-LICENSE-binary")),
               "generation ran on no archives", proc)
    finally:
        shutil.rmtree(tmp)


def case_missing_third_party():
    tmp, w = fresh_world()
    try:
        out = os.path.join(tmp, "out")
        proc = run_driver(["--tarballs", w["main"], "--repo", w["root"],
                           "--out", out])
        expect(proc.returncode == 1, "missing inventory must be red", proc)
        expect("aggregate-add-third-party" in proc.stdout + proc.stderr,
               "recovery command must be printed", proc)
    finally:
        shutil.rmtree(tmp)


def case_missing_shipped_manifest():
    tmp, w = fresh_world()
    try:
        top = os.path.join(w["build"], "hadoop-3.6.0")
        os.remove(os.path.join(top, "LICENSE-binary"))
        shutil.rmtree(os.path.join(top, "licenses-binary"))
        tar = top + ".tar.gz"
        with tarfile.open(tar, "w:gz") as tf:
            tf.add(top, arcname="hadoop-3.6.0")
        out = os.path.join(tmp, "out")
        proc = run_driver(["--tarballs", tar, "--repo", w["root"],
                           "--third-party", w["inventory"], "--out", out])
        expect(proc.returncode == 1, "manifest-less archive must be red", proc)
        expect("[coverage]" in proc.stdout, "fix list lacks R38", proc)
        expect("ships no LICENSE-binary" in proc.stdout,
               "failure must name the missing manifest", proc)
    finally:
        shutil.rmtree(tmp)


def case_verify_bad_checksum():
    tmp, w = fresh_world()
    try:
        rc = os.path.join(tmp, "rc")
        os.makedirs(rc)
        shutil.copyfile(w["main"], os.path.join(rc, "hadoop-3.6.0.tar.gz"))
        with open(os.path.join(rc, "hadoop-3.6.0.tar.gz.sha512"), "w") as fh:
            fh.write("0" * 128 + "  hadoop-3.6.0.tar.gz\n")
        shutil.copyfile(w["inventory"], os.path.join(rc, "THIRD-PARTY.txt"))
        proc = run_driver(["--verify-only", rc, "--repo", w["root"]])
        expect(proc.returncode == 1, "bad checksum must be red", proc)
        expect("[checksums]" in proc.stdout, "fix list lacks SHA512", proc)
        expect("do not trust this artifact" not in proc.stdout,
               "checksum failure must not claim signature failure", proc)
    finally:
        shutil.rmtree(tmp)


def case_verify_declared_artifacts():
    """Regression (W1 run 43338793555): verify mode must gate exactly
    the declared artifacts — an auxiliary unsigned tarball in the RC
    dir (licenses-binary.tar.gz) must not turn the verification red."""
    tmp, w = fresh_world()
    try:
        rc = os.path.join(tmp, "rc")
        os.makedirs(rc)
        for tar in (w["main"], w["cloud"]):
            shutil.copyfile(tar, os.path.join(rc, os.path.basename(tar)))
            sums = subprocess.run(["sha512sum", os.path.basename(tar)],
                                  cwd=w["build"], capture_output=True, text=True)
            with open(os.path.join(rc, os.path.basename(tar)) + ".sha512",
                      "w") as fh:
                fh.write(sums.stdout)
        shutil.copyfile(w["inventory"], os.path.join(rc, "THIRD-PARTY.txt"))
        # auxiliary, unsigned, no checksum: not a voted-on artifact
        shutil.copyfile(w["main"], os.path.join(rc, "licenses-binary.tar.gz"))
        with open(os.path.join(rc, "FORK-KEYS"), "w") as fh:
            fh.write("stub\n")
        proc = run_driver(["--verify-only", rc, "--repo", w["root"],
                           "--tarballs",
                           os.path.join(rc, "hadoop-3.6.0.tar.gz"),
                           os.path.join(rc, "hadoop-cloud-storage-dist-3.6.0.tar.gz")])
        expect(proc.returncode == 0,
               "declared-artifacts verification must be green", proc)
        expect("== licenses-binary" not in proc.stdout,
               "auxiliary tarball must not be gated", proc)
    finally:
        shutil.rmtree(tmp)


def case_verify_missing_artifacts():
    tmp, w = fresh_world()
    try:
        rc = os.path.join(tmp, "rc")
        os.makedirs(rc)
        shutil.copyfile(w["main"], os.path.join(rc, "hadoop-3.6.0.tar.gz"))
        sums = subprocess.run(["sha512sum", "hadoop-3.6.0.tar.gz"],
                              cwd=w["build"], capture_output=True, text=True)
        with open(os.path.join(rc, "hadoop-3.6.0.tar.gz.sha512"), "w") as fh:
            fh.write(sums.stdout)
        proc = run_driver(["--verify-only", rc, "--repo", w["root"]])
        expect(proc.returncode == 1, "RC without THIRD-PARTY.txt must be red", proc)
        expect("[rc-artifacts]" in proc.stdout,
               "fix list lacks MISSING-ARTIFACTS", proc)
    finally:
        shutil.rmtree(tmp)


def case_verify_green_survives_bad_repo_default():
    """--verify-only without --repo uses the script's own repo root; with
    a consistent world passed explicitly it must not depend on CWD."""
    tmp, w = fresh_world()
    try:
        rc = os.path.join(tmp, "rc")
        os.makedirs(rc)
        for tar in (w["main"], w["cloud"]):
            shutil.copyfile(tar, os.path.join(rc, os.path.basename(tar)))
            sums = subprocess.run(["sha512sum", os.path.basename(tar)],
                                  cwd=w["build"], capture_output=True, text=True)
            with open(os.path.join(rc, os.path.basename(tar)) + ".sha512",
                      "w") as fh:
                fh.write(sums.stdout)
        shutil.copyfile(w["inventory"], os.path.join(rc, "THIRD-PARTY.txt"))
        proc = subprocess.run(["bash", DRIVER, "--verify-only", rc,
                               "--repo", w["root"]],
                              capture_output=True, text=True,
                              cwd=tempfile.gettempdir())
        expect(proc.returncode == 0, "verify mode must be CWD-independent", proc)
    finally:
        shutil.rmtree(tmp)


def case_verify_sha512_tag_format():
    """create-release's signing step writes .sha512 files in sha512sum
    --tag format ('SHA512 (name) = hash'); verification must accept it
    (regression: the parser once read field 1 of the tag line, got the
    literal 'SHA512', and failed every genuine RC)."""
    tmp, w = fresh_world()
    try:
        rc = os.path.join(tmp, "rc")
        os.makedirs(rc)
        for tar in (w["main"], w["cloud"]):
            shutil.copyfile(tar, os.path.join(rc, os.path.basename(tar)))
            sums = subprocess.run(["sha512sum", os.path.basename(tar)],
                                  cwd=w["build"], capture_output=True, text=True)
            h = sums.stdout.split()[0]
            tag = "SHA512 (%s) = %s\n" % (os.path.basename(tar), h)
            with open(os.path.join(rc, os.path.basename(tar)) + ".sha512",
                      "w") as fh:
                fh.write(tag)
        shutil.copyfile(w["inventory"], os.path.join(rc, "THIRD-PARTY.txt"))
        proc = run_driver(["--verify-only", rc, "--repo", w["root"]])
        expect(proc.returncode == 0,
               "tag-format checksums must verify green", proc)
        expect("sha512 OK" in proc.stdout, "checksum result missing", proc)
    finally:
        shutil.rmtree(tmp)


def case_verify_drift_second_tarball():
    """A stale hand-edited LICENSE-binary inside the SECOND binary
    tarball must fail verification, not only drift in the first one."""
    tmp, w = fresh_world()
    try:
        rc = os.path.join(tmp, "rc")
        os.makedirs(rc)
        shutil.copyfile(w["main"], os.path.join(rc, os.path.basename(w["main"])))
        # tamper the cloud tarball's shipped manifest
        cx = os.path.join(tmp, "cloud-x")
        with tarfile.open(w["cloud"]) as tf:
            tf.extractall(cx)
        lb = None
        for dirpath, _dn, fn in os.walk(cx):
            if "LICENSE-binary" in fn:
                lb = os.path.join(dirpath, "LICENSE-binary")
                break
        assert lb, "fixture cloud tarball ships no manifest"
        with open(lb, "a") as fh:
            fh.write("# hand-edited stale line\n")
        tampered = os.path.join(rc, os.path.basename(w["cloud"]))
        with tarfile.open(tampered, "w:gz") as tf:
            tf.add(os.path.join(cx, "hadoop-cloud-storage-dist-3.6.0"),
                   arcname="hadoop-cloud-storage-dist-3.6.0")
        shutil.copyfile(w["inventory"], os.path.join(rc, "THIRD-PARTY.txt"))
        proc = run_driver(["--verify-only", rc, "--repo", w["root"]])
        expect(proc.returncode == 1,
               "drift in the second tarball must be red", proc)
        expect("[drift]" in proc.stdout,
               "fix list must carry the drift entry", proc)
    finally:
        shutil.rmtree(tmp)


CASES = [
    ("green release mode", case_green_release),
    ("two dirs after one --tarballs flag", case_two_tarball_flags),
    ("green verification mode", case_green_verify),
    ("R38: shipped jar unlicensed by shipped manifest", case_r38),
    ("R36: unnameable jar", case_r36_unnameable),
    ("R40: committed manifest drift", case_r40_drift),
    ("R53: shipped texts drift", case_r53_drift),
    ("R14: NOTICE coverage gap", case_r14_uncovered),
    ("ENUM: unenumerable archive", case_enum_failure),
    ("COLLECT: missing inventory prints recovery", case_missing_third_party),
    ("R38: archive ships no manifest", case_missing_shipped_manifest),
    ("SHA512: tampered artifact", case_verify_bad_checksum),
    ("SHA512: tag format from create-release signing", case_verify_sha512_tag_format),
    ("drift: stale manifest in the second tarball", case_verify_drift_second_tarball),
    ("MISSING-ARTIFACTS: RC without THIRD-PARTY.txt", case_verify_missing_artifacts),
    ("verify mode gates only declared artifacts", case_verify_declared_artifacts),
    ("verify mode is CWD-independent", case_verify_green_survives_bad_repo_default),
]


def main():
    failures = 0
    for name, fn in CASES:
        try:
            fn()
            print("PASS %s" % name)
        except AssertionError as exc:
            failures += 1
            print("FAIL %s\n  %s" % (name, str(exc)[:4000]))
    print("\n%d/%d cases passed" % (len(CASES) - failures, len(CASES)))
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
