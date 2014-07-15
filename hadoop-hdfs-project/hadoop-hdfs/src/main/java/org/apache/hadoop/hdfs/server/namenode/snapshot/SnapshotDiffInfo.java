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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.ChildrenDiff;
import org.apache.hadoop.hdfs.util.Diff.ListType;

import com.google.common.base.Preconditions;
import com.google.common.primitives.SignedBytes;

/**
 * A class describing the difference between snapshots of a snapshottable
 * directory.
 */
class SnapshotDiffInfo {
  /** Compare two inodes based on their full names */
  public static final Comparator<INode> INODE_COMPARATOR =
      new Comparator<INode>() {
    @Override
    public int compare(INode left, INode right) {
      if (left == null) {
        return right == null ? 0 : -1;
      } else {
        if (right == null) {
          return 1;
        } else {
          int cmp = compare(left.getParent(), right.getParent());
          return cmp == 0 ? SignedBytes.lexicographicalComparator().compare(
              left.getLocalNameBytes(), right.getLocalNameBytes()) : cmp;
        }
      }
    }
  };

  static class RenameEntry {
    private byte[][] sourcePath;
    private byte[][] targetPath;

    void setSource(INode source, byte[][] sourceParentPath) {
      Preconditions.checkState(sourcePath == null);
      sourcePath = new byte[sourceParentPath.length + 1][];
      System.arraycopy(sourceParentPath, 0, sourcePath, 0,
          sourceParentPath.length);
      sourcePath[sourcePath.length - 1] = source.getLocalNameBytes();
    }

    void setTarget(INode target, byte[][] targetParentPath) {
      targetPath = new byte[targetParentPath.length + 1][];
      System.arraycopy(targetParentPath, 0, targetPath, 0,
          targetParentPath.length);
      targetPath[targetPath.length - 1] = target.getLocalNameBytes();
    }

    void setTarget(byte[][] targetPath) {
      this.targetPath = targetPath;
    }

    boolean isRename() {
      return sourcePath != null && targetPath != null;
    }

    byte[][] getSourcePath() {
      return sourcePath;
    }

    byte[][] getTargetPath() {
      return targetPath;
    }
  }

  /** The root directory of the snapshots */
  private final INodeDirectory snapshotRoot;
  /** The starting point of the difference */
  private final Snapshot from;
  /** The end point of the difference */
  private final Snapshot to;
  /**
   * A map recording modified INodeFile and INodeDirectory and their relative
   * path corresponding to the snapshot root. Sorted based on their names.
   */
  private final SortedMap<INode, byte[][]> diffMap =
      new TreeMap<INode, byte[][]>(INODE_COMPARATOR);
  /**
   * A map capturing the detailed difference about file creation/deletion.
   * Each key indicates a directory whose children have been changed between
   * the two snapshots, while its associated value is a {@link ChildrenDiff}
   * storing the changes (creation/deletion) happened to the children (files).
   */
  private final Map<INodeDirectory, ChildrenDiff> dirDiffMap =
      new HashMap<INodeDirectory, ChildrenDiff>();

  private final Map<Long, RenameEntry> renameMap =
      new HashMap<Long, RenameEntry>();

  SnapshotDiffInfo(INodeDirectory snapshotRoot, Snapshot start, Snapshot end) {
    Preconditions.checkArgument(snapshotRoot.isSnapshottable());
    this.snapshotRoot = snapshotRoot;
    this.from = start;
    this.to = end;
  }

  /** Add a dir-diff pair */
  void addDirDiff(INodeDirectory dir, byte[][] relativePath, ChildrenDiff diff) {
    dirDiffMap.put(dir, diff);
    diffMap.put(dir, relativePath);
    // detect rename
    for (INode created : diff.getList(ListType.CREATED)) {
      if (created.isReference()) {
        RenameEntry entry = getEntry(created.getId());
        if (entry.getTargetPath() == null) {
          entry.setTarget(created, relativePath);
        }
      }
    }
    for (INode deleted : diff.getList(ListType.DELETED)) {
      if (deleted instanceof INodeReference.WithName) {
        RenameEntry entry = getEntry(deleted.getId());
        entry.setSource(deleted, relativePath);
      }
    }
  }

  Snapshot getFrom() {
    return from;
  }

  Snapshot getTo() {
    return to;
  }

  private RenameEntry getEntry(long inodeId) {
    RenameEntry entry = renameMap.get(inodeId);
    if (entry == null) {
      entry = new RenameEntry();
      renameMap.put(inodeId, entry);
    }
    return entry;
  }

  void setRenameTarget(long inodeId, byte[][] path) {
    getEntry(inodeId).setTarget(path);
  }

  /** Add a modified file */
  void addFileDiff(INodeFile file, byte[][] relativePath) {
    diffMap.put(file, relativePath);
  }

  /** @return True if {@link #from} is earlier than {@link #to} */
  boolean isFromEarlier() {
    return Snapshot.ID_COMPARATOR.compare(from, to) < 0;
  }

  /**
   * Generate a {@link SnapshotDiffReport} based on detailed diff information.
   * @return A {@link SnapshotDiffReport} describing the difference
   */
  public SnapshotDiffReport generateReport() {
    List<DiffReportEntry> diffReportList = new ArrayList<DiffReportEntry>();
    for (INode node : diffMap.keySet()) {
      diffReportList.add(new DiffReportEntry(DiffType.MODIFY, diffMap
          .get(node), null));
      if (node.isDirectory()) {
        List<DiffReportEntry> subList = generateReport(dirDiffMap.get(node),
            diffMap.get(node), isFromEarlier(), renameMap);
        diffReportList.addAll(subList);
      }
    }
    return new SnapshotDiffReport(snapshotRoot.getFullPathName(),
        Snapshot.getSnapshotName(from), Snapshot.getSnapshotName(to),
        diffReportList);
  }

  /**
   * Interpret the ChildrenDiff and generate a list of {@link DiffReportEntry}.
   * @param dirDiff The ChildrenDiff.
   * @param parentPath The relative path of the parent.
   * @param fromEarlier True indicates {@code diff=later-earlier},
   *                    False indicates {@code diff=earlier-later}
   * @param renameMap A map containing information about rename operations.
   * @return A list of {@link DiffReportEntry} as the diff report.
   */
  private List<DiffReportEntry> generateReport(ChildrenDiff dirDiff,
      byte[][] parentPath, boolean fromEarlier, Map<Long, RenameEntry> renameMap) {
    List<DiffReportEntry> list = new ArrayList<DiffReportEntry>();
    List<INode> created = dirDiff.getList(ListType.CREATED);
    List<INode> deleted = dirDiff.getList(ListType.DELETED);
    byte[][] fullPath = new byte[parentPath.length + 1][];
    System.arraycopy(parentPath, 0, fullPath, 0, parentPath.length);
    for (INode cnode : created) {
      RenameEntry entry = renameMap.get(cnode.getId());
      if (entry == null || !entry.isRename()) {
        fullPath[fullPath.length - 1] = cnode.getLocalNameBytes();
        list.add(new DiffReportEntry(fromEarlier ? DiffType.CREATE
            : DiffType.DELETE, fullPath));
      }
    }
    for (INode dnode : deleted) {
      RenameEntry entry = renameMap.get(dnode.getId());
      if (entry != null && entry.isRename()) {
        list.add(new DiffReportEntry(DiffType.RENAME,
            fromEarlier ? entry.getSourcePath() : entry.getTargetPath(),
            fromEarlier ? entry.getTargetPath() : entry.getSourcePath()));
      } else {
        fullPath[fullPath.length - 1] = dnode.getLocalNameBytes();
        list.add(new DiffReportEntry(fromEarlier ? DiffType.DELETE
            : DiffType.CREATE, fullPath));
      }
    }
    return list;
  }
}
