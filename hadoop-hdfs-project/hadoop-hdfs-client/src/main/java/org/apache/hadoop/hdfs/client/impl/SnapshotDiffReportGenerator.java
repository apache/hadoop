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
package org.apache.hadoop.hdfs.client.impl;

import java.util.*;

import com.google.common.primitives.SignedBytes;

import org.apache.hadoop.util.ChunkedArrayList;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing.DiffReportListingEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
/**
 * This class represents to end users the difference between two snapshots of
 * the same directory, or the difference between a snapshot of the directory and
 * its current state. Instead of capturing all the details of the diff, this
 * class only lists where the changes happened and their types.
 */
public class SnapshotDiffReportGenerator {
  /**
   * Compare two inodes based on their full names.
   */
  public static final Comparator<DiffReportListingEntry> INODE_COMPARATOR =
      new Comparator<DiffReportListingEntry>() {
        @Override
        public int compare(DiffReportListingEntry left,
            DiffReportListingEntry right) {
          final Comparator<byte[]> cmp =
              SignedBytes.lexicographicalComparator();
          //source path can never be null
          final byte[][] l = left.getSourcePath();
          final byte[][] r = right.getSourcePath();
          if (l.length == 1 && l[0] == null) {
            return -1;
          } else if (r.length == 1 && r[0] == null) {
            return 1;
          } else {
            for (int i = 0; i < l.length && i < r.length; i++) {
              final int diff = cmp.compare(l[i], r[i]);
              if (diff != 0) {
                return diff;
              }
            }
            return l.length == r.length ? 0 : l.length > r.length ? 1 : -1;
          }
        }
      };

  static class RenameEntry {
    private byte[][] sourcePath;
    private byte[][] targetPath;

    void setSource(byte[][] srcPath) {
      this.sourcePath = srcPath;
    }

    void setTarget(byte[][] target) {
      this.targetPath = target;
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

  /*
   * A class represnting the diff in a directory between two given snapshots
   * in two lists: createdList and deleted list.
   */
  static class ChildrenDiff {
    private final List<DiffReportListingEntry> createdList;
    private final List<DiffReportListingEntry> deletedList;

    ChildrenDiff(List<DiffReportListingEntry> createdList,
        List<DiffReportListingEntry> deletedList) {
      this.createdList = createdList != null ? createdList :
          Collections.emptyList();
      this.deletedList = deletedList != null ? deletedList :
          Collections.emptyList();
    }

    public List<DiffReportListingEntry> getCreatedList() {
      return createdList;
    }

    public List<DiffReportListingEntry> getDeletedList() {
      return deletedList;
    }
  }

  /**
   * snapshot root full path.
   */
  private final String snapshotRoot;

  /**
   * start point of the diff.
   */
  private final String fromSnapshot;

  /**
   * end point of the diff.
   */
  private final String toSnapshot;

  /**
   * Flag to indicate the diff is calculated from older to newer snapshot
   * or not.
   */
  private final boolean isFromEarlier;

  /**
   * A map capturing the detailed difference about file creation/deletion.
   * Each key indicates a directory inode whose children have been changed
   * between the two snapshots, while its associated value is a
   * {@link ChildrenDiff} storing the changes (creation/deletion) happened to
   * the children (files).
   */
  private final Map<Long, ChildrenDiff> dirDiffMap =
      new HashMap<>();

  private final Map<Long, RenameEntry> renameMap =
      new HashMap<>();

  private List<DiffReportListingEntry> mlist = null;
  private List<DiffReportListingEntry> clist = null;
  private List<DiffReportListingEntry> dlist = null;

  public SnapshotDiffReportGenerator(String snapshotRoot, String fromSnapshot,
      String toSnapshot, boolean isFromEarlier,
      List<DiffReportListingEntry> mlist, List<DiffReportListingEntry> clist,
      List<DiffReportListingEntry> dlist) {
    this.snapshotRoot = snapshotRoot;
    this.fromSnapshot = fromSnapshot;
    this.toSnapshot = toSnapshot;
    this.isFromEarlier = isFromEarlier;
    this.mlist =
        mlist != null ? mlist : Collections.emptyList();
    this.clist =
        clist != null ? clist : Collections.emptyList();
    this.dlist =
        dlist != null ? dlist : Collections.emptyList();
  }

  private RenameEntry getEntry(long inodeId) {
    RenameEntry entry = renameMap.get(inodeId);
    if (entry == null) {
      entry = new RenameEntry();
      renameMap.put(inodeId, entry);
    }
    return entry;
  }

  public void generateReportList() {
    mlist.sort(INODE_COMPARATOR);
    for (DiffReportListingEntry created : clist) {
      ChildrenDiff entry = dirDiffMap.get(created.getDirId());
      if (entry == null) {
        List<DiffReportListingEntry> createdList = new ChunkedArrayList<>();
        createdList.add(created);
        ChildrenDiff list = new ChildrenDiff(createdList, null);
        dirDiffMap.put(created.getDirId(), list);
      } else {
        dirDiffMap.get(created.getDirId()).getCreatedList().add(created);
      }
      if (created.isReference()) {
        RenameEntry renameEntry = getEntry(created.getFileId());
        if (renameEntry.getTargetPath() != null) {
          renameEntry.setTarget(created.getSourcePath());
        }
      }
    }
    for (DiffReportListingEntry deleted : dlist) {
      ChildrenDiff entry = dirDiffMap.get(deleted.getDirId());
      if (entry == null || (entry.getDeletedList().isEmpty())) {
        ChildrenDiff list;
        List<DiffReportListingEntry> deletedList = new ChunkedArrayList<>();
        deletedList.add(deleted);
        if (entry == null) {
          list = new ChildrenDiff(null, deletedList);
        } else {
          list = new ChildrenDiff(entry.getCreatedList(), deletedList);
        }
        dirDiffMap.put(deleted.getDirId(), list);
      } else {
        entry.getDeletedList().add(deleted);
      }
      if (deleted.isReference()) {
        RenameEntry renameEntry = getEntry(deleted.getFileId());
        renameEntry.setTarget(deleted.getTargetPath());
        renameEntry.setSource(deleted.getSourcePath());
      }
    }
  }

  public SnapshotDiffReport generateReport() {
    List<DiffReportEntry> diffReportList = new ChunkedArrayList<>();
    generateReportList();
    for (DiffReportListingEntry modified : mlist) {
      diffReportList.add(
          new DiffReportEntry(DiffType.MODIFY, modified.getSourcePath(), null));
      if (modified.isReference()
          && dirDiffMap.get(modified.getDirId()) != null) {
        List<DiffReportEntry> subList = generateReport(modified);
        diffReportList.addAll(subList);
      }
    }
    return new SnapshotDiffReport(snapshotRoot, fromSnapshot, toSnapshot,
        diffReportList);
  }

  private List<DiffReportEntry> generateReport(
      DiffReportListingEntry modified) {
    List<DiffReportEntry> diffReportList = new ChunkedArrayList<>();
    ChildrenDiff list = dirDiffMap.get(modified.getDirId());
    for (DiffReportListingEntry created : list.getCreatedList()) {
      RenameEntry entry = renameMap.get(created.getFileId());
      if (entry == null || !entry.isRename()) {
        diffReportList.add(new DiffReportEntry(
            isFromEarlier ? DiffType.CREATE : DiffType.DELETE,
            created.getSourcePath()));
      }
    }
    for (DiffReportListingEntry deleted : list.getDeletedList()) {
      RenameEntry entry = renameMap.get(deleted.getFileId());
      if (entry != null && entry.isRename()) {
        diffReportList.add(new DiffReportEntry(DiffType.RENAME,
            isFromEarlier ? entry.getSourcePath() : entry.getTargetPath(),
            isFromEarlier ? entry.getTargetPath() : entry.getSourcePath()));
      } else {
        diffReportList.add(new DiffReportEntry(
            isFromEarlier ? DiffType.DELETE : DiffType.CREATE,
            deleted.getSourcePath()));
      }
    }
    return diffReportList;
  }
}
