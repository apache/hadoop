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
"""Synthetic self-test for archive-delta.py. Run: python3 <this file>"""

import os
import subprocess
import sys
import tempfile

SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      "..", "scripts", "archive-delta.py")

OLD = """\
hadoop-1.0/share/hadoop/common/lib/gone-1.jar\tcom.g\tgone\t1\tpom.properties
hadoop-1.0/share/hadoop/common/lib/bump-1.jar\tcom.b\tbump\t1\tpom.properties
hadoop-1.0/share/hadoop/hdfs/lib/two-1.jar\tcom.t\ttwo\t1\tpom.properties
hadoop-1.0/share/hadoop/common/lib/dup-1.jar\tcom.d\tdup\t1\tpom.properties
hadoop-1.0/share/hadoop/hdfs/lib/dup-1.jar\tcom.d\tdup\t1\tpom.properties
hadoop-1.0/share/hadoop/yarn/app-1.0.war#WEB-INF/lib/w-1.jar\tcom.w\tw\t1\tpom.properties
hadoop-1.0/share/hadoop/common/hadoop-common-1.0.jar\torg.apache.hadoop\thadoop-common\t1.0\tpom.properties
"""
NEW = """\
hadoop-2.0/share/hadoop/common/lib/new-1.jar\tcom.n\tnew\t1\tpom.properties
hadoop-2.0/share/hadoop/common/lib/bump-2.jar\tcom.b\tbump\t2\tpom.properties
hadoop-2.0/share/hadoop/hdfs/lib/two-1.jar\tcom.t\ttwo\t1\tpom.properties
hadoop-2.0/share/hadoop/hdfs/lib/two-2.jar\tcom.t\ttwo\t2\tpom.properties
hadoop-2.0/share/hadoop/common/lib/dup-1.jar\tcom.d\tdup\t1\tpom.properties
hadoop-2.0/share/hadoop/yarn/app-2.0.war#WEB-INF/lib/w-1.jar\tcom.w\tw\t1\tpom.properties
hadoop-2.0/share/hadoop/common/hadoop-common-2.0.jar\torg.apache.hadoop\thadoop-common\t2.0\tpom.properties
"""


def main():
    with tempfile.TemporaryDirectory() as d:
        for side, name, tsv in (("old", "hadoop-1.0", OLD), ("new", "hadoop-2.0", NEW)):
            os.makedirs(os.path.join(d, side, name))
            open(os.path.join(d, side, name, "archive-jars.tsv"), "w").write(tsv)
        os.makedirs(os.path.join(d, "new", "extra"))
        open(os.path.join(d, "new", "extra", "archive-jars.tsv"), "w").write(NEW)
        maps = os.path.join(d, "maps")
        open(maps, "w").write("")
        p = subprocess.run([sys.executable, "-I", SCRIPT, d + "/old", d + "/new",
                            "--mappings", maps], capture_output=True, text=True)
        assert p.returncode == 0, p.stderr
        lines = set(p.stdout.splitlines())
        assert "- com.g:gone:1  (share/hadoop/common/lib)" in lines, p.stdout
        assert "+ com.n:new:1  (share/hadoop/common/lib)" in lines, p.stdout
        assert "~ com.b:bump  1 -> 2" in lines, p.stdout
        assert "    - 1  (share/hadoop/common/lib)" in lines, p.stdout
        assert "    + 2  (share/hadoop/common/lib)" in lines, p.stdout
        # a second version next to the first: where the new one ships
        assert "~ com.t:two  1 -> 1,2" in lines, p.stdout
        assert "    + 2  (share/hadoop/hdfs/lib)" in lines, p.stdout
        assert ("# com.d:dup:1  share/hadoop/common/lib, share/hadoop/hdfs/lib"
                " -> share/hadoop/common/lib") in lines, p.stdout
        # own jars are left out; the release version in WAR names is no move
        assert "hadoop-common" not in p.stdout and "com.w" not in p.stdout, p.stdout
        assert "## hadoop-1.0 -> hadoop-2.0 (5 changes)" in lines, p.stdout
        assert "## extra: new archive (no baseline)" in lines, p.stdout
        p = subprocess.run([sys.executable, "-I", SCRIPT, d + "/old", d + "/none",
                            "--mappings", maps], capture_output=True, text=True)
        assert p.returncode == 2, p.stderr
    print("test-archive-delta: OK")


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
