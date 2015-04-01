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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * This class provides the basic functionality to sync two FileSystems based on
 * the snapshot diff report. More specifically, we have the following settings:
 * 1. Both the source and target FileSystem must be DistributedFileSystem
 * 2. Two snapshots (e.g., s1 and s2) have been created on the source FS.
 * The diff between these two snapshots will be copied to the target FS.
 * 3. The target has the same snapshot s1. No changes have been made on the
 * target since s1. All the files/directories in the target are the same with
 * source.s1
 */
class DistCpSync {

  static boolean sync(DistCpOptions inputOptions, Configuration conf)
      throws IOException {
    List<Path> sourcePaths = inputOptions.getSourcePaths();
    if (sourcePaths.size() != 1) {
      // we only support one source dir which must be a snapshottable directory
      DistCp.LOG.warn(sourcePaths.size() + " source paths are provided");
      return false;
    }
    final Path sourceDir = sourcePaths.get(0);
    final Path targetDir = inputOptions.getTargetPath();

    final FileSystem sfs = sourceDir.getFileSystem(conf);
    final FileSystem tfs = targetDir.getFileSystem(conf);
    // currently we require both the source and the target file system are
    // DistributedFileSystem.
    if (!(sfs instanceof DistributedFileSystem) ||
        !(tfs instanceof DistributedFileSystem)) {
      DistCp.LOG.warn("To use diff-based distcp, the FileSystems needs to" +
          " be DistributedFileSystem");
      return false;
    }
    final DistributedFileSystem sourceFs = (DistributedFileSystem) sfs;
    final DistributedFileSystem targetFs= (DistributedFileSystem) tfs;

    // make sure targetFS has no change between from and the current states
    if (!checkNoChange(inputOptions, targetFs, targetDir)) {
      return false;
    }

    Path tmpDir = null;
    try {
      tmpDir = createTargetTmpDir(targetFs, targetDir);
      DiffInfo[] diffs = getDiffs(inputOptions, sourceFs, sourceDir, targetDir);
      if (diffs == null) {
        return false;
      }
      // do the real sync work: deletion and rename
      syncDiff(diffs, targetFs, tmpDir);
      return true;
    } catch (Exception e) {
      DistCp.LOG.warn("Failed to use snapshot diff for distcp", e);
      return false;
    } finally {
      deleteTargetTmpDir(targetFs, tmpDir);
      // TODO: since we have tmp directory, we can support "undo" with failures
      // set the source path using the snapshot path
      inputOptions.setSourcePaths(Arrays.asList(getSourceSnapshotPath(sourceDir,
          inputOptions.getToSnapshot())));
    }
  }

  private static String getSnapshotName(String name) {
    return Path.CUR_DIR.equals(name) ? "" : name;
  }

  private static Path getSourceSnapshotPath(Path sourceDir, String snapshotName) {
    if (Path.CUR_DIR.equals(snapshotName)) {
      return sourceDir;
    } else {
      return new Path(sourceDir,
          HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR + snapshotName);
    }
  }

  private static Path createTargetTmpDir(DistributedFileSystem targetFs,
      Path targetDir) throws IOException {
    final Path tmp = new Path(targetDir,
        DistCpConstants.HDFS_DISTCP_DIFF_DIRECTORY_NAME + DistCp.rand.nextInt());
    if (!targetFs.mkdirs(tmp)) {
      throw new IOException("The tmp directory " + tmp + " already exists");
    }
    return tmp;
  }

  private static void deleteTargetTmpDir(DistributedFileSystem targetFs,
      Path tmpDir) {
    try {
      if (tmpDir != null) {
        targetFs.delete(tmpDir, true);
      }
    } catch (IOException e) {
      DistCp.LOG.error("Unable to cleanup tmp dir: " + tmpDir, e);
    }
  }

  /**
   * Compute the snapshot diff on the given file system. Return true if the diff
   * is empty, i.e., no changes have happened in the FS.
   */
  private static boolean checkNoChange(DistCpOptions inputOptions,
      DistributedFileSystem fs, Path path) {
    try {
      SnapshotDiffReport targetDiff =
          fs.getSnapshotDiffReport(path, inputOptions.getFromSnapshot(), "");
      if (!targetDiff.getDiffList().isEmpty()) {
        DistCp.LOG.warn("The target has been modified since snapshot "
            + inputOptions.getFromSnapshot());
        return false;
      } else {
        return true;
      }
    } catch (IOException e) {
      DistCp.LOG.warn("Failed to compute snapshot diff on " + path, e);
    }
    return false;
  }

  @VisibleForTesting
  static DiffInfo[] getDiffs(DistCpOptions inputOptions,
      DistributedFileSystem fs, Path sourceDir, Path targetDir) {
    try {
      final String from = getSnapshotName(inputOptions.getFromSnapshot());
      final String to = getSnapshotName(inputOptions.getToSnapshot());
      SnapshotDiffReport sourceDiff = fs.getSnapshotDiffReport(sourceDir,
          from, to);
      return DiffInfo.getDiffs(sourceDiff, targetDir);
    } catch (IOException e) {
      DistCp.LOG.warn("Failed to compute snapshot diff on " + sourceDir, e);
    }
    return null;
  }

  private static void syncDiff(DiffInfo[] diffs,
      DistributedFileSystem targetFs, Path tmpDir) throws IOException {
    moveToTmpDir(diffs, targetFs, tmpDir);
    moveToTarget(diffs, targetFs);
  }

  /**
   * Move all the source files that should be renamed or deleted to the tmp
   * directory.
   */
  private static void moveToTmpDir(DiffInfo[] diffs,
      DistributedFileSystem targetFs, Path tmpDir) throws IOException {
    // sort the diffs based on their source paths to make sure the files and
    // subdirs are moved before moving their parents/ancestors.
    Arrays.sort(diffs, DiffInfo.sourceComparator);
    Random random = new Random();
    for (DiffInfo diff : diffs) {
      Path tmpTarget = new Path(tmpDir, diff.source.getName());
      while (targetFs.exists(tmpTarget)) {
        tmpTarget = new Path(tmpDir, diff.source.getName() + random.nextInt());
      }
      diff.setTmp(tmpTarget);
      targetFs.rename(diff.source, tmpTarget);
    }
  }

  /**
   * Finish the rename operations: move all the intermediate files/directories
   * from the tmp dir to the final targets.
   */
  private static void moveToTarget(DiffInfo[] diffs,
      DistributedFileSystem targetFs) throws IOException {
    // sort the diffs based on their target paths to make sure the parent
    // directories are created first.
    Arrays.sort(diffs, DiffInfo.targetComparator);
    for (DiffInfo diff : diffs) {
      if (diff.target != null) {
        if (!targetFs.exists(diff.target.getParent())) {
          targetFs.mkdirs(diff.target.getParent());
        }
        targetFs.rename(diff.getTmp(), diff.target);
      }
    }
  }
}
