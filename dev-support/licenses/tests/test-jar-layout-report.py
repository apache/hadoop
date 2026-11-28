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
"""Synthetic self-test for jar-layout-report.py. Run: python3 <this file>"""

import os
import subprocess
import sys
import tempfile

SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      "..", "scripts", "jar-layout-report.py")

TSV = """\
h/common/lib/failureaccess-1.0.2.jar\tcom.google.guava\tfailureaccess\t1.0.2\tpom.properties
h/common/lib/failureaccess-1.0.3.jar\tcom.google.guava\tfailureaccess\t1.0.3\tpom.properties
h/hdfs/lib/failureaccess-1.0.3.jar\tcom.google.guava\tfailureaccess\t1.0.3\tpom.properties
h/common/lib/guava-33.jar\tcom.google.guava\tguava\t33\tpom.properties
h/hdfs/lib/guava-33.jar\tcom.google.guava\tguava\t33\tpom.properties
h/app.war#WEB-INF/lib/guava-33.jar\tcom.google.guava\tguava\t33\tpom.properties
h/csi/lib/grpc-api-1.69.jar\t\tgrpc-api\t1.69\tmanifest
h/common/lib/grpc-api-1.70.jar\tio.grpc\tgrpc-api\t1.70\tpom.properties
h/common/hadoop-common-3.6.jar\torg.apache.hadoop\thadoop-common\t3.6\tpom.properties
h/hdfs/hadoop-common-3.6.jar\torg.apache.hadoop\thadoop-common\t3.6\tpom.properties
h/common/lib/mystery.jar\t\t\t?\tfilename
"""


def run(tsv, root=None):
    with tempfile.TemporaryDirectory() as d:
        t = os.path.join(d, "a.tsv")
        m = os.path.join(d, "maps")
        open(t, "w").write(tsv)
        open(m, "w").write("grpc-api io.grpc:grpc-api # partial row\n")
        cmd = [sys.executable, "-I", SCRIPT, t, "--mappings", m]
        if root:
            cmd += ["--root", root]
        p = subprocess.run(cmd, capture_output=True, text=True)
        return p.returncode, p.stdout, p.stderr


def main():
    with tempfile.TemporaryDirectory() as root:
        for rel, size in [("h/common/lib/guava-33.jar", 3 * 1048576),
                          ("h/hdfs/lib/guava-33.jar", 3 * 1048576)]:
            os.makedirs(os.path.dirname(os.path.join(root, rel)), exist_ok=True)
            with open(os.path.join(root, rel), "wb") as fh:
                fh.truncate(size)
        rc, out, err = run(TSV, root)
    assert rc == 0, err
    # own jars dropped, unresolved row listed and not counted
    assert "8 third-party jar files, 5 distinct g:a:v, 4 lib" in out, out
    assert "hadoop-common" not in out, out
    assert "resist identification (1)" in out and "mystery.jar" in out, out
    # 1. conflict only where both versions share a directory
    assert "conflicts in one lib directory (1)" in out, out
    assert "`com.google.guava:failureaccess` 1.0.2, 1.0.3 in `h/common/lib`" in out, out
    # 2. split includes the partial row promoted through the mapping
    assert "several versions across the archive (2)" in out, out
    assert "`io.grpc:grpc-api`: 1.69 (`h/csi/lib`); 1.70 (`h/common/lib`)" in out, out
    # 3. grouping by directory set; size counts on-disk copies beyond one
    assert "across lib directories (2 g:a:v)" in out, out
    assert ("### 1 shared by `h/app.war#WEB-INF/lib` + `h/common/lib` + "
            "`h/hdfs/lib`, redundant on-disk copies 3.0 MiB") in out, out
    assert "### 1 shared by `h/common/lib` + `h/hdfs/lib`\n" in out, out
    # malformed input fails closed
    rc, _, err = run("only\ttwo\n")
    assert rc == 2 and "malformed TSV row" in err, (rc, err)
    print("test-jar-layout-report: OK")


def _default_curation_paths():
    """The script's default --mappings/--keep resolve inside the
    real checkout's curation/ sibling directory (reorg guard)."""
    import os as _os
    sdir = _os.path.dirname(_os.path.abspath(__file__))
    for name in ["archive-jar-mappings"]:
        p = _os.path.normpath(_os.path.join(sdir, "..", "curation", name))
        assert _os.path.isfile(p), \
            "default curation path does not resolve: " + p


_default_curation_paths()


if __name__ == "__main__":
    main()
