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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.tools.CopyListing.InvalidInputException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.EnumMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Collections;

/**
 * This class provides the basic functionality to sync two FileSystems based on
 * the snapshot diff report. More specifically, we have the following settings:
 * 1. Both the source and target FileSystem must be DistributedFileSystem or
 * (s)WebHdfsFileSystem
 * 2. Two snapshots (e.g., s1 and s2) have been created on the source FS.
 * The diff between these two snapshots will be copied to the target FS.
 * 3. The target has the same snapshot s1. No changes have been made on the
 * target since s1. All the files/directories in the target are the same with
 * source.s1
 */
class DistCpSync {
  private DistCpContext context;
  private Configuration conf;
  // diffMap maps snapshot diff op type to a list of diff ops.
  // It's initially created based on the snapshot diff. Then the individual
  // diff stored there maybe modified instead of copied by the distcp algorithm
  // afterwards, for better performance.
  //
  private EnumMap<SnapshotDiffReport.DiffType, List<DiffInfo>> diffMap;
  private DiffInfo[] renameDiffs;
  // entries which are marked deleted because of rename to a excluded target
  // path
  private List<DiffInfo> deletedByExclusionDiffs;
  private CopyFilter copyFilter;

  DistCpSync(DistCpContext context, Configuration conf) {
    this.context = context;
    this.conf = conf;
    this.copyFilter = CopyFilter.getCopyFilter(conf);
    this.copyFilter.initialize();
  }

  @VisibleForTesting
  public void setCopyFilter(CopyFilter copyFilter) {
    this.copyFilter = copyFilter;
  }

  private boolean isRdiff() {
    return context.shouldUseRdiff();
  }

  /**
   * Check if three conditions are met before sync.
   * 1. Only one source directory.
   * 2. Both source and target file system are DFS or WebHdfs.
   * 3. There is no change between from and the current status in target
   *    file system.
   *  Throw exceptions if first two aren't met, and return false to fallback to
   *  default distcp if the third condition isn't met.
   */
  private boolean preSyncCheck() throws IOException {
    List<Path> sourcePaths = context.getSourcePaths();
    if (sourcePaths.size() != 1) {
      // we only support one source dir which must be a snapshottable directory
      throw new IllegalArgumentException(sourcePaths.size()
          + " source paths are provided");
    }
    final Path sourceDir = sourcePaths.get(0);
    final Path targetDir = context.getTargetPath();

    final FileSystem srcFs = sourceDir.getFileSystem(conf);
    final FileSystem tgtFs = targetDir.getFileSystem(conf);
    final FileSystem snapshotDiffFs = isRdiff() ? tgtFs : srcFs;
    final Path snapshotDiffDir = isRdiff() ? targetDir : sourceDir;

    checkFilesystemSupport(sourceDir,targetDir,srcFs, tgtFs);

    // make sure targetFS has no change between from and the current states
    if (!checkNoChange(tgtFs, targetDir)) {
      // set the source path using the snapshot path
      context.setSourcePaths(Arrays.asList(getSnapshotPath(sourceDir,
          context.getToSnapshot())));
      return false;
    }

    final String from = getSnapshotName(
        context.getFromSnapshot());
    final String to = getSnapshotName(
        context.getToSnapshot());

    try {
      final FileStatus fromSnapshotStat =
          snapshotDiffFs.getFileStatus(getSnapshotPath(snapshotDiffDir, from));

      final FileStatus toSnapshotStat =
          snapshotDiffFs.getFileStatus(getSnapshotPath(snapshotDiffDir, to));

      if (isRdiff()) {
        // If fromSnapshot isn't current dir then do a time check
        if (!from.equals("")
            && fromSnapshotStat.getModificationTime() < toSnapshotStat
            .getModificationTime()) {
          throw new HadoopIllegalArgumentException("Snapshot " + from
              + " should be newer than " + to);
        }
      } else {
        // If toSnapshot isn't current dir then do a time check
        if(!to.equals("")
            && fromSnapshotStat.getModificationTime() > toSnapshotStat
            .getModificationTime()) {
          throw new HadoopIllegalArgumentException("Snapshot " + to
              + " should be newer than " + from);
        }
      }
    } catch (FileNotFoundException nfe) {
      throw new InvalidInputException("Input snapshot is not found", nfe);
    }

    return true;
  }

  /**
   * Check if the source and target filesystems support snapshots.
   */
  private void checkFilesystemSupport(Path sourceDir, Path targetDir,
      FileSystem srcFs, FileSystem tgtFs) throws IOException {
    if (!srcFs.hasPathCapability(sourceDir,
        CommonPathCapabilities.FS_SNAPSHOTS)) {
      throw new UnsupportedOperationException(
          "The source file system " + srcFs.getScheme()
              + " does not support snapshot.");
    }
    if (!tgtFs.hasPathCapability(targetDir,
        CommonPathCapabilities.FS_SNAPSHOTS)) {
      throw new UnsupportedOperationException(
          "The target file system " + tgtFs.getScheme()
              + " does not support snapshot.");
    }
    try {
      getSnapshotDiffReportMethod(srcFs);
    } catch (NoSuchMethodException e) {
      throw new UnsupportedOperationException(
          "The source file system " + srcFs.getScheme()
              + " does not support getSnapshotDiffReport",
          e);
    }
    try {
      getSnapshotDiffReportMethod(tgtFs);
    } catch (NoSuchMethodException e) {
      throw new UnsupportedOperationException(
          "The target file system " + tgtFs.getScheme()
              + " does not support getSnapshotDiffReport",
          e);
    }

  }

  public boolean sync() throws IOException {
    if (!preSyncCheck()) {
      return false;
    }

    if (!getAllDiffs()) {
      return false;
    }

    List<Path> sourcePaths = context.getSourcePaths();
    final Path sourceDir = sourcePaths.get(0);
    final Path targetDir = context.getTargetPath();
    final FileSystem tfs = targetDir.getFileSystem(conf);

    Path tmpDir = null;
    try {
      tmpDir = createTargetTmpDir(tfs, targetDir);
      DiffInfo[] renameAndDeleteDiffs =
          getRenameAndDeleteDiffsForSync(targetDir);
      if (renameAndDeleteDiffs.length > 0) {
        // do the real sync work: deletion and rename
        syncDiff(renameAndDeleteDiffs, tfs, tmpDir);
      }
      return true;
    } catch (Exception e) {
      DistCp.LOG.warn("Failed to use snapshot diff for distcp", e);
      return false;
    } finally {
      deleteTargetTmpDir(tfs, tmpDir);
      // TODO: since we have tmp directory, we can support "undo" with failures
      // set the source path using the snapshot path
      context.setSourcePaths(Arrays.asList(getSnapshotPath(sourceDir,
          context.getToSnapshot())));
    }
  }

  /**
   * Get all diffs from source directory snapshot diff report, put them into an
   * EnumMap whose key is DiffType, and value is a DiffInfo list. If there is
   * no entry for a given DiffType, the associated value will be an empty list.
   */
  private boolean getAllDiffs() throws IOException {
    Path ssDir = isRdiff()?
        context.getTargetPath() : context.getSourcePaths().get(0);

    try {
      final String from = getSnapshotName(context.getFromSnapshot());
      final String to = getSnapshotName(context.getToSnapshot());
      SnapshotDiffReport report =
          getSnapshotDiffReport(ssDir.getFileSystem(conf), ssDir, from, to);

      this.diffMap = new EnumMap<>(SnapshotDiffReport.DiffType.class);
      for (SnapshotDiffReport.DiffType type :
          SnapshotDiffReport.DiffType.values()) {
        diffMap.put(type, new ArrayList<DiffInfo>());
      }
      deletedByExclusionDiffs = null;
      for (SnapshotDiffReport.DiffReportEntry entry : report.getDiffList()) {
        // If the entry is the snapshot root, usually a item like "M\t."
        // in the diff report. We don't need to handle it and cannot handle it,
        // since its sourcepath is empty.
        if (entry.getSourcePath().length <= 0) {
          continue;
        }
        SnapshotDiffReport.DiffType dt = entry.getType();
        List<DiffInfo> list = diffMap.get(dt);
        final Path source =
                new Path(DFSUtilClient.bytes2String(entry.getSourcePath()));
        final Path relativeSource = new Path(Path.SEPARATOR + source);
        if (dt == SnapshotDiffReport.DiffType.MODIFY ||
            dt == SnapshotDiffReport.DiffType.CREATE ||
            dt == SnapshotDiffReport.DiffType.DELETE) {
          if (copyFilter.shouldCopy(relativeSource)) {
            list.add(new DiffInfo(source, null, dt));
          }
        } else if (dt == SnapshotDiffReport.DiffType.RENAME) {
          final Path target =
                  new Path(DFSUtilClient.bytes2String(entry.getTargetPath()));
          final Path relativeTarget = new Path(Path.SEPARATOR + target);
          if (copyFilter.shouldCopy(relativeSource)) {
            if (copyFilter.shouldCopy(relativeTarget)) {
              list.add(new DiffInfo(source, target, dt));
            } else {
              list = diffMap.get(SnapshotDiffReport.DiffType.DELETE);
              DiffInfo info = new DiffInfo(source, null,
                  SnapshotDiffReport.DiffType.DELETE);
              list.add(info);
              if (deletedByExclusionDiffs == null) {
                deletedByExclusionDiffs = new ArrayList<>();
              }
              deletedByExclusionDiffs.add(info);
            }
          } else if (copyFilter.shouldCopy(relativeTarget)) {
            list = diffMap.get(SnapshotDiffReport.DiffType.CREATE);
            list.add(new DiffInfo(target, null,
                    SnapshotDiffReport.DiffType.CREATE));
          }
        }
      }
      if (deletedByExclusionDiffs != null) {
        Collections.sort(deletedByExclusionDiffs, DiffInfo.sourceComparator);
      }
      return true;
    } catch (IOException e) {
      DistCp.LOG.warn("Failed to compute snapshot diff on " + ssDir, e);
    }
    this.diffMap = null;
    return false;
  }

  /**
   * Check if the filesystem implementation has a method named
   * getSnapshotDiffReport.
   */
  private static Method getSnapshotDiffReportMethod(FileSystem fs)
      throws NoSuchMethodException {
    return fs.getClass().getMethod(
        "getSnapshotDiffReport", Path.class, String.class, String.class);
  }

  /**
   * Get the snapshotDiff b/w the fromSnapshot & toSnapshot for the given
   * filesystem.
   */
  private static SnapshotDiffReport getSnapshotDiffReport(
      final FileSystem fs,
      final Path snapshotDir,
      final String fromSnapshot,
      final String toSnapshot) throws IOException {
    try {
      return (SnapshotDiffReport) getSnapshotDiffReportMethod(fs).invoke(
          fs, snapshotDir, fromSnapshot, toSnapshot);
    } catch (InvocationTargetException e) {
      throw new IOException(e.getCause());
    } catch (NoSuchMethodException|IllegalAccessException e) {
      throw new IllegalArgumentException(
          "Failed to invoke getSnapshotDiffReport.", e);
    }
  }

  private String getSnapshotName(String name) {
    return Path.CUR_DIR.equals(name) ? "" : name;
  }

  private Path getSnapshotPath(Path inputDir, String snapshotName) {
    if (Path.CUR_DIR.equals(snapshotName)) {
      return inputDir;
    } else {
      return new Path(inputDir,
          HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR + snapshotName);
    }
  }

  private Path createTargetTmpDir(FileSystem targetFs,
                                  Path targetDir) throws IOException {
    final Path tmp = new Path(targetDir,
        DistCpConstants.HDFS_DISTCP_DIFF_DIRECTORY_NAME + DistCp.rand.nextInt());
    if (!targetFs.mkdirs(tmp)) {
      throw new IOException("The tmp directory " + tmp + " already exists");
    }
    return tmp;
  }

  private void deleteTargetTmpDir(FileSystem targetFs,
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
  private boolean checkNoChange(FileSystem fs, Path path) {
    try {
      final String from = getSnapshotName(context.getFromSnapshot());
      SnapshotDiffReport targetDiff = getSnapshotDiffReport(fs, path, from, "");
      if (!targetDiff.getDiffList().isEmpty()) {
        DistCp.LOG.warn("The target has been modified since snapshot "
            + context.getFromSnapshot());
        return false;
      } else {
        return true;
      }
    } catch (IOException e) {
      DistCp.LOG.warn("Failed to compute snapshot diff on " + path
          + " at snapshot " + context.getFromSnapshot(), e);
    }
    return false;
  }

  private void syncDiff(DiffInfo[] diffs,
      FileSystem targetFs, Path tmpDir) throws IOException {
    moveToTmpDir(diffs, targetFs, tmpDir);
    moveToTarget(diffs, targetFs);
  }

  /**
   * Move all the source files that should be renamed or deleted to the tmp
   * directory.
   */
  private void moveToTmpDir(DiffInfo[] diffs,
      FileSystem targetFs, Path tmpDir) throws IOException {
    // sort the diffs based on their source paths to make sure the files and
    // subdirs are moved before moving their parents/ancestors.
    Arrays.sort(diffs, DiffInfo.sourceComparator);
    Random random = new Random();
    for (DiffInfo diff : diffs) {
      Path tmpTarget = new Path(tmpDir, diff.getSource().getName());
      while (targetFs.exists(tmpTarget)) {
        tmpTarget = new Path(tmpDir,
            diff.getSource().getName() + random.nextInt());
      }
      diff.setTmp(tmpTarget);
      targetFs.rename(diff.getSource(), tmpTarget);
    }
  }

  /**
   * Finish the rename operations: move all the intermediate files/directories
   * from the tmp dir to the final targets.
   */
  private void moveToTarget(DiffInfo[] diffs,
      FileSystem targetFs) throws IOException {
    // sort the diffs based on their target paths to make sure the parent
    // directories are created first.
    Arrays.sort(diffs, DiffInfo.targetComparator);
    for (DiffInfo diff : diffs) {
      if (diff.getTarget() != null) {
        targetFs.mkdirs(diff.getTarget().getParent());
        targetFs.rename(diff.getTmp(), diff.getTarget());
      }
    }
  }

  /**
   * Get rename and delete diffs and add the targetDir as the prefix of their
   * source and target paths.
   */
  private DiffInfo[] getRenameAndDeleteDiffsForSync(Path targetDir) {
    // NOTE: when HDFS-10263 is done, getRenameAndDeleteDiffsRdiff
    // should be the same as getRenameAndDeleteDiffsFdiff. Specifically,
    // we should just move the body of getRenameAndDeleteDiffsFdiff
    // to here and remove both getRenameAndDeleteDiffsFdiff
    // and getRenameAndDeleteDiffsDdiff.
    if (isRdiff()) {
      return getRenameAndDeleteDiffsRdiff(targetDir);
    } else {
      return getRenameAndDeleteDiffsFdiff(targetDir);
    }
  }

  /**
   * Get rename and delete diffs and add the targetDir as the prefix of their
   * source and target paths.
   */
  private DiffInfo[] getRenameAndDeleteDiffsRdiff(Path targetDir) {
    List<DiffInfo> renameDiffsList =
        diffMap.get(SnapshotDiffReport.DiffType.RENAME);

    // Prepare a renameDiffArray for translating deleted items below.
    // Do a reversion here due to HDFS-10263.
    List<DiffInfo> renameDiffsListReversed =
        new ArrayList<DiffInfo>(renameDiffsList.size());
    for (DiffInfo diff : renameDiffsList) {
      renameDiffsListReversed.add(new DiffInfo(diff.getTarget(),
          diff.getSource(), diff.getType()));
    }
    DiffInfo[] renameDiffArray =
        renameDiffsListReversed.toArray(new DiffInfo[renameDiffsList.size()]);

    Arrays.sort(renameDiffArray, DiffInfo.sourceComparator);

    List<DiffInfo> renameAndDeleteDiff = new ArrayList<>();
    // Traverse DELETE list, which we need to delete them in sync process.
    // Use the renameDiffArray prepared to translate the path.
    for (DiffInfo diff : diffMap.get(SnapshotDiffReport.DiffType.DELETE)) {
      DiffInfo renameItem = getRenameItem(diff, renameDiffArray);
      Path source;
      if (renameItem != null) {
        source = new Path(targetDir,
            translateRenamedPath(diff.getSource(), renameItem));
      } else {
        source = new Path(targetDir, diff.getSource());
      }
      renameAndDeleteDiff.add(new DiffInfo(source, null,
          SnapshotDiffReport.DiffType.DELETE));
    }
    for (DiffInfo diff : diffMap.get(SnapshotDiffReport.DiffType.RENAME)) {
      // swap target and source here for Rdiff
      Path source = new Path(targetDir, diff.getSource());
      Path target = new Path(targetDir, diff.getTarget());
      renameAndDeleteDiff.add(new DiffInfo(source, target, diff.getType()));
    }
    return renameAndDeleteDiff.toArray(
        new DiffInfo[renameAndDeleteDiff.size()]);
  }

    /**
   * Get rename and delete diffs and add the targetDir as the prefix of their
   * source and target paths.
   */
  private DiffInfo[] getRenameAndDeleteDiffsFdiff(Path targetDir) {
    List<DiffInfo> renameAndDeleteDiff = new ArrayList<>();
    for (DiffInfo diff : diffMap.get(SnapshotDiffReport.DiffType.DELETE)) {
      Path source = new Path(targetDir, diff.getSource());
      renameAndDeleteDiff.add(new DiffInfo(source, diff.getTarget(),
          diff.getType()));
    }

    for (DiffInfo diff : diffMap.get(SnapshotDiffReport.DiffType.RENAME)) {
      Path source = new Path(targetDir, diff.getSource());
      Path target = new Path(targetDir, diff.getTarget());
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
      if (diff.getSource().equals(renameItem.getSource())) {
        // The same path string may appear in:
        // 1. both renamed and modified snapshot diff entries.
        // 2. both renamed and created snapshot diff entries.
        // Case 1 is the about same file/directory, whereas case 2
        // is about two different files/directories.
        // We are finding case 1 here, thus we check against DiffType.MODIFY.
        if (diff.getType() == SnapshotDiffReport.DiffType.MODIFY) {
          return renameItem;
        }
      } else if (isParentOf(renameItem.getSource(), diff.getSource())) {
        // If rename entry is the parent of diff entry, then both MODIFY and
        // CREATE diff entries should be handled.
        return renameItem;
      }
    }
    return null;
  }

  /**
   * checks if a parent dir is marked deleted as a part of dir rename happening
   * to a path which is excluded by the the filter.
   * @return true if it's marked deleted
   */
  private boolean isParentOrSelfMarkedDeleted(DiffInfo diff,
      List<DiffInfo> deletedDirDiffArray) {
    for (DiffInfo item : deletedDirDiffArray) {
      if (item.getSource().equals(diff.getSource())) {
        // The same path string may appear in:
        // 1. both deleted and modified snapshot diff entries.
        // 2. both deleted and created snapshot diff entries.
        // Case 1 is the about same file/directory, whereas case 2
        // is about two different files/directories.
        // We are finding case 1 here, thus we check against DiffType.MODIFY.
        if (diff.getType() == SnapshotDiffReport.DiffType.MODIFY) {
          return true;
        }
      } else if (isParentOf(item.getSource(), diff.getSource())) {
        // If deleted entry is the parent of diff entry, then both MODIFY and
        // CREATE diff entries should be handled.
        return true;
      }
    }
    return false;
  }

  /**
   * For a given sourcePath, get its real path if it or its parent was renamed.
   *
   * For example, if we renamed dirX to dirY, and created dirY/fileX,
   * the initial snapshot diff would be a CREATE snapshot diff that looks like
   *   + dirX/fileX
   * The rename snapshot diff looks like
   *   R dirX dirY
   *
   * We convert the soucePath dirX/fileX to dirY/fileX here.
   *
   * @return target path
   */
  private Path translateRenamedPath(Path sourcePath,
      DiffInfo renameItem) {
    if (sourcePath.equals(renameItem.getSource())) {
      return renameItem.getTarget();
    }
    StringBuffer sb = new StringBuffer(sourcePath.toString());
    String remain =
        sb.substring(renameItem.getSource().toString().length() + 1);
    return new Path(renameItem.getTarget(), remain);
  }

  /**
   * Prepare the diff list.
   * This diff list only includes created or modified files/directories, since
   * delete and rename items are synchronized already.
   *
   * If the parent or self of a source path is renamed, we need to change its
   * target path according the correspondent rename item.
   *
   * For RDiff usage, the diff.getSource() is what we will use as its target
   * path.
   *
   * @return a diff list
   */
  public ArrayList<DiffInfo> prepareDiffListForCopyListing() {
    DiffInfo[] modifyAndCreateDiffs = getCreateAndModifyDiffs();
    ArrayList<DiffInfo> finalListWithTarget = new ArrayList<>();
    if (isRdiff()) {
      for (DiffInfo diff : modifyAndCreateDiffs) {
        diff.setTarget(diff.getSource());
        finalListWithTarget.add(diff);
      }
    } else {
      List<DiffInfo> renameDiffsList =
          diffMap.get(SnapshotDiffReport.DiffType.RENAME);
      DiffInfo[] renameDiffArray =
          renameDiffsList.toArray(new DiffInfo[renameDiffsList.size()]);
      Arrays.sort(renameDiffArray, DiffInfo.sourceComparator);
      for (DiffInfo diff : modifyAndCreateDiffs) {
        //  In cases, where files/dirs got created after a snapshot is taken
        // and then the parent dir is moved to location which is excluded by
        // the filters. For example, files/dirs created inside a dir in an
        // encryption zone in HDFS. When the parent dir gets deleted, it will be
        // moved to trash within which is inside the encryption zone itself.
        // If the trash path gets excluded by filters , the dir will be marked
        // for DELETE for the target location. All the subsequent creates should
        // for such dirs should be ignored as well as the modify operation
        // on the dir itself.
        if (deletedByExclusionDiffs != null && isParentOrSelfMarkedDeleted(diff,
            deletedByExclusionDiffs)) {
          continue;
        }
        DiffInfo renameItem = getRenameItem(diff, renameDiffArray);
        if (renameItem == null) {
          diff.setTarget(diff.getSource());
        } else {
          diff.setTarget(translateRenamedPath(diff.getSource(), renameItem));
        }
        finalListWithTarget.add(diff);
      }
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
      if (isParentOf(newDir, diff.getTarget())) {
        foundChild = true;
        excludeList.add(new Path(prefix, diff.getTarget()).toUri().getPath());
      } else if (foundChild) {
        // The renameDiffs was sorted, the matching section should be
        // contiguous.
        break;
      }
    }
    return excludeList;
  }
}
