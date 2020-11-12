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

import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing.DiffReportListingEntry;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.ChildrenDiff;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.util.ChunkedArrayList;

/**
 * A class describing the difference between snapshots of a snapshottable
 * directory where the difference is limited by dfs.snapshotDiff-report.limit.
 */

class SnapshotDiffListingInfo {
  private final int maxEntries;

  /** The root directory of the snapshots. */
  private final INodeDirectory snapshotRoot;
  /**
   *  The scope directory under which snapshot diff is calculated.
   */
  private final INodeDirectory snapshotDiffScopeDir;
  /** The starting point of the difference. */
  private final Snapshot from;
  /** The end point of the difference. */
  private final Snapshot to;

  /** The path of the file to start for computing the snapshot diff. */
  private byte[] lastPath = DFSUtilClient.EMPTY_BYTES;

  private int lastIndex = -1;

  /*
   * A list containing all the modified entries between the given snapshots
   * within a single rpc call.
   */
  private final List<DiffReportListingEntry> modifiedList =
      new ChunkedArrayList<>();

  private final List<DiffReportListingEntry> createdList =
      new ChunkedArrayList<>();

  private final List<DiffReportListingEntry> deletedList =
      new ChunkedArrayList<>();

  SnapshotDiffListingInfo(INodeDirectory snapshotRootDir,
      INodeDirectory snapshotDiffScopeDir, Snapshot start, Snapshot end,
      int snapshotDiffReportLimit) {
    Preconditions.checkArgument(
        snapshotRootDir.isSnapshottable() && snapshotDiffScopeDir
            .isDescendantOfSnapshotRoot(snapshotRootDir));
    this.snapshotRoot = snapshotRootDir;
    this.snapshotDiffScopeDir = snapshotDiffScopeDir;
    this.from = start;
    this.to = end;
    this.maxEntries = snapshotDiffReportLimit;
  }

  boolean addDirDiff(long dirId, byte[][] parent, ChildrenDiff diff) {
    final Snapshot laterSnapshot = getLater();
    if (lastIndex == -1) {
      if (getTotalEntries() < maxEntries) {
        modifiedList.add(new DiffReportListingEntry(
            dirId, dirId, parent, true, null));
      } else {
        setLastPath(parent);
        setLastIndex(-1);
        return false;
      }
    }

    final List<INode> clist =  diff.getCreatedUnmodifiable();
    if (lastIndex == -1 || lastIndex < clist.size()) {
      ListIterator<INode> iterator = lastIndex != -1 ?
          clist.listIterator(lastIndex): clist.listIterator();
      while (iterator.hasNext()) {
        if (getTotalEntries() < maxEntries) {
          INode created = iterator.next();
          byte[][] path = newPath(parent, created.getLocalNameBytes());
          createdList.add(new DiffReportListingEntry(dirId, created.getId(),
              path, created.isReference(), null));
        } else {
          setLastPath(parent);
          setLastIndex(iterator.nextIndex());
          return false;
        }
      }
      setLastIndex(-1);
    }

    if (lastIndex == -1 || lastIndex >= clist.size()) {
      final List<INode> dlist =  diff.getDeletedUnmodifiable();
      int size = dlist.size();
      ListIterator<INode> iterator = lastIndex != -1 ?
          dlist.listIterator(lastIndex - size): dlist.listIterator();
      while (iterator.hasNext()) {
        if (getTotalEntries() < maxEntries) {
          final INode d = iterator.next();
          byte[][] path = newPath(parent, d.getLocalNameBytes());
          byte[][] target = findRenameTargetPath(d, laterSnapshot);
          final DiffReportListingEntry e = target != null ?
              new DiffReportListingEntry(dirId, d.getId(), path, true, target) :
              new DiffReportListingEntry(dirId, d.getId(), path, false, null);
          deletedList.add(e);
        } else {
          setLastPath(parent);
          setLastIndex(size + iterator.nextIndex());
          return false;
        }
      }
      setLastIndex(-1);
    }
    return true;
  }

  private byte[][] findRenameTargetPath(INode deleted, Snapshot laterSnapshot) {
    if (deleted instanceof INodeReference.WithName) {
      return snapshotRoot.getDirectorySnapshottableFeature()
          .findRenameTargetPath(snapshotDiffScopeDir,
              (INodeReference.WithName) deleted,
              Snapshot.getSnapshotId(laterSnapshot));
    }
    return null;
  }

  private static byte[][] newPath(byte[][] parent, byte[] name) {
    byte[][] fullPath = new byte[parent.length + 1][];
    System.arraycopy(parent, 0, fullPath, 0, parent.length);
    fullPath[fullPath.length - 1] = name;
    return fullPath;
  }

  Snapshot getEarlier() {
    return isFromEarlier()? from: to;
  }

  Snapshot getLater() {
    return isFromEarlier()? to: from;
  }


  public void setLastPath(byte[][] lastPath) {
    this.lastPath = DFSUtilClient.byteArray2bytes(lastPath);
  }

  public void setLastIndex(int idx) {
    this.lastIndex = idx;
  }

  boolean addFileDiff(INodeFile file, byte[][] relativePath) {
    if (getTotalEntries() < maxEntries) {
      modifiedList.add(new DiffReportListingEntry(file.getId(),
          file.getId(), relativePath,false, null));
    } else {
      setLastPath(relativePath);
      return false;
    }
    return true;
  }
  /** @return True if {@link #from} is earlier than {@link #to} */
  boolean isFromEarlier() {
    return Snapshot.ID_COMPARATOR.compare(from, to) < 0;
  }


  private int getTotalEntries() {
    return createdList.size() + modifiedList.size() + deletedList.size();
  }

  /**
   * Generate a {@link SnapshotDiffReportListing} based on detailed diff
   * information.
   *
   * @return A {@link SnapshotDiffReportListing} describing the difference
   */
  public SnapshotDiffReportListing generateReport() {
    return new SnapshotDiffReportListing(lastPath, modifiedList, createdList,
        deletedList, lastIndex, isFromEarlier());
  }
}
