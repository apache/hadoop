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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.EnumMap;
import java.util.ArrayList;
import java.util.HashSet;

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
  private DistCpOptions inputOptions;
  private Configuration conf;
  private EnumMap<SnapshotDiffReport.DiffType, List<DiffInfo>> diffMap;
  private DiffInfo[] renameDiffs;

  DistCpSync(DistCpOptions options, Configuration conf) {
    this.inputOptions = options;
    this.conf = conf;
  }

  /**
   * Check if three conditions are met before sync.
   * 1. Only one source directory.
   * 2. Both source and target file system are DFS.
   * 3. There is no change between from and the current status in target
   *    file system.
   *  Throw exceptions if first two aren't met, and return false to fallback to
   *  default distcp if the third condition isn't met.
   */
  private boolean preSyncCheck() throws IOException {
    List<Path> sourcePaths = inputOptions.getSourcePaths();
    if (sourcePaths.size() != 1) {
      // we only support one source dir which must be a snapshottable directory
      throw new IllegalArgumentException(sourcePaths.size()
          + " source paths are provided");
    }
    final Path sourceDir = sourcePaths.get(0);
    final Path targetDir = inputOptions.getTargetPath();

    final FileSystem sfs = sourceDir.getFileSystem(conf);
    final FileSystem tfs = targetDir.getFileSystem(conf);
    // currently we require both the source and the target file system are
    // DistributedFileSystem.
    if (!(sfs instanceof DistributedFileSystem) ||
        !(tfs instanceof DistributedFileSystem)) {
      throw new IllegalArgumentException("The FileSystems needs to" +
          " be DistributedFileSystem for using snapshot-diff-based distcp");
    }
    final DistributedFileSystem targetFs = (DistributedFileSystem) tfs;

    // make sure targetFS has no change between from and the current states
    if (!checkNoChange(targetFs, targetDir)) {
      // set the source path using the snapshot path
      inputOptions.setSourcePaths(Arrays.asList(getSourceSnapshotPath(sourceDir,
          inputOptions.getToSnapshot())));
      return false;
    }
    return true;
  }

  public boolean sync() throws IOException {
    if (!preSyncCheck()) {
      return false;
    }

    if (!getAllDiffs()) {
      return false;
    }

    List<Path> sourcePaths = inputOptions.getSourcePaths();
    final Path sourceDir = sourcePaths.get(0);
    final Path targetDir = inputOptions.getTargetPath();
    final FileSystem tfs = targetDir.getFileSystem(conf);
    final DistributedFileSystem targetFs = (DistributedFileSystem) tfs;

    Path tmpDir = null;
    try {
      tmpDir = createTargetTmpDir(targetFs, targetDir);
      DiffInfo[] renameAndDeleteDiffs = getRenameAndDeleteDiffs(targetDir);
      if (renameAndDeleteDiffs.length > 0) {
        // do the real sync work: deletion and rename
        syncDiff(renameAndDeleteDiffs, targetFs, tmpDir);
      }
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

  /**
   * Get all diffs from source directory snapshot diff report, put them into an
   * EnumMap whose key is DiffType, and value is a DiffInfo list. If there is
   * no entry for a given DiffType, the associated value will be an empty list.
   */
  private boolean getAllDiffs() throws IOException {
    List<Path> sourcePaths = inputOptions.getSourcePaths();
    final Path sourceDir = sourcePaths.get(0);
    try {
      DistributedFileSystem fs =
          (DistributedFileSystem) sourceDir.getFileSystem(conf);
      final String from = getSnapshotName(inputOptions.getFromSnapshot());
      final String to = getSnapshotName(inputOptions.getToSnapshot());
      SnapshotDiffReport report = fs.getSnapshotDiffReport(sourceDir,
          from, to);

      this.diffMap = new EnumMap<>(SnapshotDiffReport.DiffType.class);
      for (SnapshotDiffReport.DiffType type :
          SnapshotDiffReport.DiffType.values()) {
        diffMap.put(type, new ArrayList<DiffInfo>());
      }

      for (SnapshotDiffReport.DiffReportEntry entry : report.getDiffList()) {
        // If the entry is the snapshot root, usually a item like "M\t."
        // in the diff report. We don't need to handle it and cannot handle it,
        // since its sourcepath is empty.
        if (entry.getSourcePath().length <= 0) {
          continue;
        }
        List<DiffInfo> list = diffMap.get(entry.getType());

        if (entry.getType() == SnapshotDiffReport.DiffType.MODIFY ||
            entry.getType() == SnapshotDiffReport.DiffType.CREATE ||
            entry.getType() == SnapshotDiffReport.DiffType.DELETE) {
          final Path source =
              new Path(DFSUtil.bytes2String(entry.getSourcePath()));
          list.add(new DiffInfo(source, null, entry.getType()));
        } else if (entry.getType() == SnapshotDiffReport.DiffType.RENAME) {
          final Path source =
              new Path(DFSUtil.bytes2String(entry.getSourcePath()));
          final Path target =
              new Path(DFSUtil.bytes2String(entry.getTargetPath()));
          list.add(new DiffInfo(source, target, entry.getType()));
        }
      }
      return true;
    } catch (IOException e) {
      DistCp.LOG.warn("Failed to compute snapshot diff on " + sourceDir, e);
    }
    this.diffMap = null;
    return false;
  }

  private String getSnapshotName(String name) {
    return Path.CUR_DIR.equals(name) ? "" : name;
  }

  private Path getSourceSnapshotPath(Path sourceDir, String snapshotName) {
    if (Path.CUR_DIR.equals(snapshotName)) {
      return sourceDir;
    } else {
      return new Path(sourceDir,
          HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR + snapshotName);
    }
  }

  private Path createTargetTmpDir(DistributedFileSystem targetFs,
                                  Path targetDir) throws IOException {
    final Path tmp = new Path(targetDir,
        DistCpConstants.HDFS_DISTCP_DIFF_DIRECTORY_NAME + DistCp.rand.nextInt());
    if (!targetFs.mkdirs(tmp)) {
      throw new IOException("The tmp directory " + tmp + " already exists");
    }
    return tmp;
  }

  private void deleteTargetTmpDir(DistributedFileSystem targetFs,
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
  private boolean checkNoChange(DistributedFileSystem fs, Path path) {
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

  private void syncDiff(DiffInfo[] diffs,
      DistributedFileSystem targetFs, Path tmpDir) throws IOException {
    moveToTmpDir(diffs, targetFs, tmpDir);
    moveToTarget(diffs, targetFs);
  }

  /**
   * Move all the source files that should be renamed or deleted to the tmp
   * directory.
   */
  private void moveToTmpDir(DiffInfo[] diffs,
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
  private void moveToTarget(DiffInfo[] diffs,
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

  /**
   * Get rename and delete diffs and add the targetDir as the prefix of their
   * source and target paths.
   */
  private DiffInfo[] getRenameAndDeleteDiffs(Path targetDir) {
    List<DiffInfo> renameAndDeleteDiff = new ArrayList<>();
    for (DiffInfo diff : diffMap.get(SnapshotDiffReport.DiffType.DELETE)) {
      Path source = new Path(targetDir, diff.source);
      renameAndDeleteDiff.add(new DiffInfo(source, diff.target,
          diff.getType()));
    }

    for (DiffInfo diff : diffMap.get(SnapshotDiffReport.DiffType.RENAME)) {
      Path source = new Path(targetDir, diff.source);
      Path target = new Path(targetDir, diff.target);
      renameAndDeleteDiff.add(new DiffInfo(source, target, diff.getType()));
    }

    return renameAndDeleteDiff.toArray(
        new DiffInfo[renameAndDeleteDiff.size()]);
  }

  private DiffInfo[] getCreateAndModifyDiffs() {
    List<DiffInfo> createDiff =
        diffMap.get(SnapshotDiffReport.DiffType.CREATE);
    List<DiffInfo> modifyDiff =
        diffMap.get(SnapshotDiffReport.DiffType.MODIFY);
    List<DiffInfo> diffs =
        new ArrayList<>(createDiff.size() + modifyDiff.size());
    diffs.addAll(createDiff);
    diffs.addAll(modifyDiff);
    return diffs.toArray(new DiffInfo[diffs.size()]);
  }

  /**
   * Probe for a path being a parent of another.
   * @return true if the parent's path matches the start of the child's
   */
  private boolean isParentOf(Path parent, Path child) {
    String parentPath = parent.toString();
    String childPath = child.toString();
    if (!parentPath.endsWith(Path.SEPARATOR)) {
      parentPath += Path.SEPARATOR;
    }

    return childPath.length() > parentPath.length() &&
        childPath.startsWith(parentPath);
  }

  /**
   * Find the possible rename item which equals to the parent or self of
   * a created/modified file/directory.
   * @param diff a modify/create diff item
   * @param renameDiffArray all rename diffs
   * @return possible rename item
   */
  private DiffInfo getRenameItem(DiffInfo diff, DiffInfo[] renameDiffArray) {
    for (DiffInfo renameItem : renameDiffArray) {
      if (diff.source.equals(renameItem.source)) {
        // The same path string may appear in:
        // 1. both renamed and modified snapshot diff entries.
        // 2. both renamed and created snapshot diff entries.
        // Case 1 is the about same file/directory, whereas case 2
        // is about two different files/directories.
        // We are finding case 1 here, thus we check against DiffType.MODIFY.
        if (diff.getType() == SnapshotDiffReport.DiffType.MODIFY) {
          return renameItem;
        }
      } else if (isParentOf(renameItem.source, diff.source)) {
        // If rename entry is the parent of diff entry, then both MODIFY and
        // CREATE diff entries should be handled.
        return renameItem;
      }
    }
    return null;
  }

  /**
   * For a given source path, get its target path based on the rename item.
   * @return target path
   */
  private Path getTargetPath(Path sourcePath, DiffInfo renameItem) {
    if (sourcePath.equals(renameItem.source)) {
      return renameItem.target;
    }
    StringBuffer sb = new StringBuffer(sourcePath.toString());
    String remain = sb.substring(renameItem.source.toString().length() + 1);
    return new Path(renameItem.target, remain);
  }

  /**
   * Prepare the diff list.
   * This diff list only includes created or modified files/directories, since
   * delete and rename items are synchronized already.
   *
   * If the parent or self of a source path is renamed, we need to change its
   * target path according the correspondent rename item.
   * @return a diff list
   */
  public ArrayList<DiffInfo> prepareDiffList() {
    DiffInfo[] modifyAndCreateDiffs = getCreateAndModifyDiffs();

    List<DiffInfo> renameDiffsList =
        diffMap.get(SnapshotDiffReport.DiffType.RENAME);
    DiffInfo[] renameDiffArray =
        renameDiffsList.toArray(new DiffInfo[renameDiffsList.size()]);
    Arrays.sort(renameDiffArray, DiffInfo.sourceComparator);

    ArrayList<DiffInfo> finalListWithTarget = new ArrayList<>();
    for (DiffInfo diff : modifyAndCreateDiffs) {
      DiffInfo renameItem = getRenameItem(diff, renameDiffArray);
      if (renameItem == null) {
        diff.target = diff.source;
      } else {
        diff.target = getTargetPath(diff.source, renameItem);
      }
      finalListWithTarget.add(diff);
    }
    return finalListWithTarget;
  }

  /**
   * This method returns a list of items to be excluded when recursively
   * traversing newDir to build the copy list.
   *
   * Specifically, given a newly created directory newDir (a CREATE entry in
   * the snapshot diff), if a previously copied file/directory itemX is moved
   * (a RENAME entry in the snapshot diff) into newDir, itemX should be
   * excluded when recursively traversing newDir in caller method so that it
   * will not to be copied again.
   * If the same itemX also has a MODIFY entry in the snapshot diff report,
   * meaning it was modified after it was previously copied, it will still
   * be added to the copy list in caller method.
   * @return the exclude list
   */
  public HashSet<String> getTraverseExcludeList(Path newDir, Path prefix) {
    if (renameDiffs == null) {
      List<DiffInfo> renameList =
          diffMap.get(SnapshotDiffReport.DiffType.RENAME);
      renameDiffs = renameList.toArray(new DiffInfo[renameList.size()]);
      Arrays.sort(renameDiffs, DiffInfo.targetComparator);
    }

    if (renameDiffs.length <= 0) {
      return null;
    }

    boolean foundChild = false;
    HashSet<String> excludeList = new HashSet<>();
    for (DiffInfo diff : renameDiffs) {
      if (isParentOf(newDir, diff.target)) {
        foundChild = true;
        excludeList.add(new Path(prefix, diff.target).toUri().getPath());
      } else if (foundChild) {
        // The renameDiffs was sorted, the matching section should be
        // contiguous.
        break;
      }
    }
    return excludeList;
  }
}
