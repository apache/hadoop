/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.tools;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;

/**
 * Information presenting a rename/delete op derived from a snapshot diff entry.
 * This includes the source file/dir of the rename/delete op, and the target
 * file/dir of a rename op.
 */
class DiffInfo {
  static final Comparator<DiffInfo> sourceComparator = new Comparator<DiffInfo>() {
    @Override
    public int compare(DiffInfo d1, DiffInfo d2) {
      return d2.source.compareTo(d1.source);
    }
  };

  static final Comparator<DiffInfo> targetComparator = new Comparator<DiffInfo>() {
    @Override
    public int compare(DiffInfo d1, DiffInfo d2) {
      return d1.target == null ? -1 :
          (d2.target ==  null ? 1 : d1.target.compareTo(d2.target));
    }
  };

  /** The source file/dir of the rename or deletion op */
  final Path source;
  /**
   * The intermediate file/dir for the op. For a rename or a delete op,
   * we first rename the source to this tmp file/dir.
   */
  private Path tmp;
  /** The target file/dir of the rename op. Null means the op is deletion. */
  final Path target;

  DiffInfo(Path source, Path target) {
    assert source != null;
    this.source = source;
    this.target= target;
  }

  void setTmp(Path tmp) {
    this.tmp = tmp;
  }

  Path getTmp() {
    return tmp;
  }

  static DiffInfo[] getDiffs(SnapshotDiffReport report, Path targetDir) {
    List<DiffInfo> diffs = new ArrayList<>();
    for (SnapshotDiffReport.DiffReportEntry entry : report.getDiffList()) {
      if (entry.getType() == SnapshotDiffReport.DiffType.DELETE) {
        final Path source = new Path(targetDir,
            DFSUtil.bytes2String(entry.getSourcePath()));
        diffs.add(new DiffInfo(source, null));
      } else if (entry.getType() == SnapshotDiffReport.DiffType.RENAME) {
        final Path source = new Path(targetDir,
            DFSUtil.bytes2String(entry.getSourcePath()));
        final Path target = new Path(targetDir,
            DFSUtil.bytes2String(entry.getTargetPath()));
        diffs.add(new DiffInfo(source, target));
      }
    }
    return diffs.toArray(new DiffInfo[diffs.size()]);
  }
}
