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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.namenode.Content;
import org.apache.hadoop.hdfs.server.namenode.ContentSummaryComputationContext;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeMap;
import org.apache.hadoop.hdfs.server.namenode.Quota;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.ChildrenDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiff;
import org.apache.hadoop.hdfs.util.Diff.ListType;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.util.Time;

import com.google.common.base.Preconditions;
import com.google.common.primitives.SignedBytes;

/**
 * Directories where taking snapshots is allowed.
 * 
 * Like other {@link INode} subclasses, this class is synchronized externally
 * by the namesystem and FSDirectory locks.
 */
@InterfaceAudience.Private
public class INodeDirectorySnapshottable extends INodeDirectory {
  /** Limit the number of snapshot per snapshottable directory. */
  static final int SNAPSHOT_LIMIT = 1 << 16;

  /** Cast INode to INodeDirectorySnapshottable. */
  static public INodeDirectorySnapshottable valueOf(
      INode inode, String src) throws IOException {
    final INodeDirectory dir = INodeDirectory.valueOf(inode, src);
    if (!dir.isSnapshottable()) {
      throw new SnapshotException(
          "Directory is not a snapshottable directory: " + src);
    }
    return (INodeDirectorySnapshottable)dir;
  }
  
  /**
   * A class describing the difference between snapshots of a snapshottable
   * directory.
   */
  public static class SnapshotDiffInfo {
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
    
    /** The root directory of the snapshots */
    private final INodeDirectorySnapshottable snapshotRoot;
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
    
    SnapshotDiffInfo(INodeDirectorySnapshottable snapshotRoot, Snapshot start,
        Snapshot end) {
      this.snapshotRoot = snapshotRoot;
      this.from = start;
      this.to = end;
    }
    
    /** Add a dir-diff pair */
    private void addDirDiff(INodeDirectory dir, byte[][] relativePath,
        ChildrenDiff diff) {
      dirDiffMap.put(dir, diff);
      diffMap.put(dir, relativePath);
    }
    
    /** Add a modified file */ 
    private void addFileDiff(INodeFile file, byte[][] relativePath) {
      diffMap.put(file, relativePath);
    }
    
    /** @return True if {@link #from} is earlier than {@link #to} */
    private boolean isFromEarlier() {
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
            .get(node)));
        if (node.isDirectory()) {
          ChildrenDiff dirDiff = dirDiffMap.get(node);
          List<DiffReportEntry> subList = dirDiff.generateReport(
              diffMap.get(node), isFromEarlier());
          diffReportList.addAll(subList);
        }
      }
      return new SnapshotDiffReport(snapshotRoot.getFullPathName(),
          Snapshot.getSnapshotName(from), Snapshot.getSnapshotName(to),
          diffReportList);
    }
  }

  /**
   * Snapshots of this directory in ascending order of snapshot names.
   * Note that snapshots in ascending order of snapshot id are stored in
   * {@link INodeDirectoryWithSnapshot}.diffs (a private field).
   */
  private final List<Snapshot> snapshotsByNames = new ArrayList<Snapshot>();

  /**
   * @return {@link #snapshotsByNames}
   */
  ReadOnlyList<Snapshot> getSnapshotsByNames() {
    return ReadOnlyList.Util.asReadOnlyList(this.snapshotsByNames);
  }
  
  /** Number of snapshots allowed. */
  private int snapshotQuota = SNAPSHOT_LIMIT;

  public INodeDirectorySnapshottable(INodeDirectory dir) {
    super(dir, true, dir.getFeatures());
    // add snapshot feature if the original directory does not have it
    if (!isWithSnapshot()) {
      addSnapshotFeature(null);
    }
  }
  
  /** @return the number of existing snapshots. */
  public int getNumSnapshots() {
    return snapshotsByNames.size();
  }
  
  private int searchSnapshot(byte[] snapshotName) {
    return Collections.binarySearch(snapshotsByNames, snapshotName);
  }

  /** @return the snapshot with the given name. */
  public Snapshot getSnapshot(byte[] snapshotName) {
    final int i = searchSnapshot(snapshotName);
    return i < 0? null: snapshotsByNames.get(i);
  }
  
  Snapshot getSnapshotById(int sid) {
    for (Snapshot s : snapshotsByNames) {
      if (s.getId() == sid) {
        return s;
      }
    }
    return null;
  }
  
  /** @return {@link #snapshotsByNames} as a {@link ReadOnlyList} */
  public ReadOnlyList<Snapshot> getSnapshotList() {
    return ReadOnlyList.Util.asReadOnlyList(snapshotsByNames);
  }
  
  /**
   * Rename a snapshot
   * @param path
   *          The directory path where the snapshot was taken. Used for
   *          generating exception message.
   * @param oldName
   *          Old name of the snapshot
   * @param newName
   *          New name the snapshot will be renamed to
   * @throws SnapshotException
   *           Throw SnapshotException when either the snapshot with the old
   *           name does not exist or a snapshot with the new name already
   *           exists
   */
  public void renameSnapshot(String path, String oldName, String newName)
      throws SnapshotException {
    if (newName.equals(oldName)) {
      return;
    }
    final int indexOfOld = searchSnapshot(DFSUtil.string2Bytes(oldName));
    if (indexOfOld < 0) {
      throw new SnapshotException("The snapshot " + oldName
          + " does not exist for directory " + path);
    } else {
      final byte[] newNameBytes = DFSUtil.string2Bytes(newName);
      int indexOfNew = searchSnapshot(newNameBytes);
      if (indexOfNew > 0) {
        throw new SnapshotException("The snapshot " + newName
            + " already exists for directory " + path);
      }
      // remove the one with old name from snapshotsByNames
      Snapshot snapshot = snapshotsByNames.remove(indexOfOld);
      final INodeDirectory ssRoot = snapshot.getRoot();
      ssRoot.setLocalName(newNameBytes);
      indexOfNew = -indexOfNew - 1;
      if (indexOfNew <= indexOfOld) {
        snapshotsByNames.add(indexOfNew, snapshot);
      } else { // indexOfNew > indexOfOld
        snapshotsByNames.add(indexOfNew - 1, snapshot);
      }
    }
  }

  public int getSnapshotQuota() {
    return snapshotQuota;
  }

  public void setSnapshotQuota(int snapshotQuota) {
    if (snapshotQuota < 0) {
      throw new HadoopIllegalArgumentException(
          "Cannot set snapshot quota to " + snapshotQuota + " < 0");
    }
    this.snapshotQuota = snapshotQuota;
  }

  @Override
  public boolean isSnapshottable() {
    return true;
  }
  
  /**
   * Simply add a snapshot into the {@link #snapshotsByNames}. Used by FSImage
   * loading.
   */
  void addSnapshot(Snapshot snapshot) {
    this.snapshotsByNames.add(snapshot);
  }

  /** Add a snapshot. */
  Snapshot addSnapshot(int id, String name) throws SnapshotException,
      QuotaExceededException {
    //check snapshot quota
    final int n = getNumSnapshots();
    if (n + 1 > snapshotQuota) {
      throw new SnapshotException("Failed to add snapshot: there are already "
          + n + " snapshot(s) and the snapshot quota is "
          + snapshotQuota);
    }
    final Snapshot s = new Snapshot(id, name, this);
    final byte[] nameBytes = s.getRoot().getLocalNameBytes();
    final int i = searchSnapshot(nameBytes);
    if (i >= 0) {
      throw new SnapshotException("Failed to add snapshot: there is already a "
          + "snapshot with the same name \"" + Snapshot.getSnapshotName(s) + "\".");
    }

    final DirectoryDiff d = getDiffs().addDiff(id, this);
    d.setSnapshotRoot(s.getRoot());
    snapshotsByNames.add(-i - 1, s);

    //set modification time
    updateModificationTime(Time.now(), Snapshot.CURRENT_STATE_ID);
    s.getRoot().setModificationTime(getModificationTime(),
        Snapshot.CURRENT_STATE_ID);
    return s;
  }
  
  /**
   * Remove the snapshot with the given name from {@link #snapshotsByNames},
   * and delete all the corresponding DirectoryDiff.
   * 
   * @param snapshotName The name of the snapshot to be removed
   * @param collectedBlocks Used to collect information to update blocksMap
   * @return The removed snapshot. Null if no snapshot with the given name 
   *         exists.
   */
  Snapshot removeSnapshot(String snapshotName,
      BlocksMapUpdateInfo collectedBlocks, final List<INode> removedINodes)
      throws SnapshotException {
    final int i = searchSnapshot(DFSUtil.string2Bytes(snapshotName));
    if (i < 0) {
      throw new SnapshotException("Cannot delete snapshot " + snapshotName
          + " from path " + this.getFullPathName()
          + ": the snapshot does not exist.");
    } else {
      final Snapshot snapshot = snapshotsByNames.get(i);
      int prior = Snapshot.findLatestSnapshot(this, snapshot.getId());
      try {
        Quota.Counts counts = cleanSubtree(snapshot.getId(), prior,
            collectedBlocks, removedINodes, true);
        INodeDirectory parent = getParent();
        if (parent != null) {
          // there will not be any WithName node corresponding to the deleted 
          // snapshot, thus only update the quota usage in the current tree
          parent.addSpaceConsumed(-counts.get(Quota.NAMESPACE),
              -counts.get(Quota.DISKSPACE), true);
        }
      } catch(QuotaExceededException e) {
        LOG.error("BUG: removeSnapshot increases namespace usage.", e);
      }
      // remove from snapshotsByNames after successfully cleaning the subtree
      snapshotsByNames.remove(i);
      return snapshot;
    }
  }
  
  @Override
  public ContentSummaryComputationContext computeContentSummary(
      final ContentSummaryComputationContext summary) {
    super.computeContentSummary(summary);
    summary.getCounts().add(Content.SNAPSHOT, snapshotsByNames.size());
    summary.getCounts().add(Content.SNAPSHOTTABLE_DIRECTORY, 1);
    return summary;
  }

  /**
   * Compute the difference between two snapshots (or a snapshot and the current
   * directory) of the directory.
   * 
   * @param from The name of the start point of the comparison. Null indicating
   *          the current tree.
   * @param to The name of the end point. Null indicating the current tree.
   * @return The difference between the start/end points.
   * @throws SnapshotException If there is no snapshot matching the starting
   *           point, or if endSnapshotName is not null but cannot be identified
   *           as a previous snapshot.
   */
  SnapshotDiffInfo computeDiff(final String from, final String to)
      throws SnapshotException {
    Snapshot fromSnapshot = getSnapshotByName(from);
    Snapshot toSnapshot = getSnapshotByName(to);
    // if the start point is equal to the end point, return null
    if (from.equals(to)) {
      return null;
    }
    SnapshotDiffInfo diffs = new SnapshotDiffInfo(this, fromSnapshot,
        toSnapshot);
    computeDiffRecursively(this, new ArrayList<byte[]>(), diffs);
    return diffs;
  }
  
  /**
   * Find the snapshot matching the given name.
   * 
   * @param snapshotName The name of the snapshot.
   * @return The corresponding snapshot. Null if snapshotName is null or empty.
   * @throws SnapshotException If snapshotName is not null or empty, but there
   *           is no snapshot matching the name.
   */
  private Snapshot getSnapshotByName(String snapshotName)
      throws SnapshotException {
    Snapshot s = null;
    if (snapshotName != null && !snapshotName.isEmpty()) {
      final int index = searchSnapshot(DFSUtil.string2Bytes(snapshotName));
      if (index < 0) {
        throw new SnapshotException("Cannot find the snapshot of directory "
            + this.getFullPathName() + " with name " + snapshotName);
      }
      s = snapshotsByNames.get(index);
    }
    return s;
  }
  
  /**
   * Recursively compute the difference between snapshots under a given
   * directory/file.
   * @param node The directory/file under which the diff is computed. 
   * @param parentPath Relative path (corresponding to the snapshot root) of 
   *                   the node's parent.
   * @param diffReport data structure used to store the diff.
   */
  private void computeDiffRecursively(INode node, List<byte[]> parentPath,
      SnapshotDiffInfo diffReport) {
    ChildrenDiff diff = new ChildrenDiff();
    byte[][] relativePath = parentPath.toArray(new byte[parentPath.size()][]);
    if (node.isDirectory()) {
      INodeDirectory dir = node.asDirectory();
      DirectoryWithSnapshotFeature sf = dir.getDirectoryWithSnapshotFeature();
      if (sf != null) {
        boolean change = sf.computeDiffBetweenSnapshots(diffReport.from,
            diffReport.to, diff, dir);
        if (change) {
          diffReport.addDirDiff(dir, relativePath, diff);
        }
      }
      ReadOnlyList<INode> children = dir.getChildrenList(
          diffReport.isFromEarlier() ? Snapshot.getSnapshotId(diffReport.to) : 
            Snapshot.getSnapshotId(diffReport.from));
      for (INode child : children) {
        final byte[] name = child.getLocalNameBytes();
        if (diff.searchIndex(ListType.CREATED, name) < 0
            && diff.searchIndex(ListType.DELETED, name) < 0) {
          parentPath.add(name);
          computeDiffRecursively(child, parentPath, diffReport);
          parentPath.remove(parentPath.size() - 1);
        }
      }
    } else if (node.isFile() && node.asFile().isWithSnapshot()) {
      INodeFile file = node.asFile();
      Snapshot earlierSnapshot = diffReport.isFromEarlier() ? diffReport.from
          : diffReport.to;
      Snapshot laterSnapshot = diffReport.isFromEarlier() ? diffReport.to
          : diffReport.from;
      boolean change = file.getDiffs().changedBetweenSnapshots(earlierSnapshot,
          laterSnapshot);
      if (change) {
        diffReport.addFileDiff(file, relativePath);
      }
    }
  }
  
  /**
   * Replace itself with {@link INodeDirectoryWithSnapshot} or
   * {@link INodeDirectory} depending on the latest snapshot.
   */
  INodeDirectory replaceSelf(final int latestSnapshotId, final INodeMap inodeMap)
      throws QuotaExceededException {
    if (latestSnapshotId == Snapshot.CURRENT_STATE_ID) {
      Preconditions.checkState(getDirectoryWithSnapshotFeature()
          .getLastSnapshotId() == Snapshot.CURRENT_STATE_ID, "this=%s", this);
    }
    INodeDirectory dir = replaceSelf4INodeDirectory(inodeMap);
    if (latestSnapshotId != Snapshot.CURRENT_STATE_ID) {
      dir.recordModification(latestSnapshotId);
    }
    return dir;
  }

  @Override
  public String toDetailString() {
    return super.toDetailString() + ", snapshotsByNames=" + snapshotsByNames;
  }

  @Override
  public void dumpTreeRecursively(PrintWriter out, StringBuilder prefix,
      int snapshot) {
    super.dumpTreeRecursively(out, prefix, snapshot);

    if (snapshot == Snapshot.CURRENT_STATE_ID) {
      out.println();
      out.print(prefix);

      out.print("Snapshot of ");
      final String name = getLocalName();
      out.print(name.isEmpty()? "/": name);
      out.print(": quota=");
      out.print(getSnapshotQuota());

      int n = 0;
      for(DirectoryDiff diff : getDiffs()) {
        if (diff.isSnapshotRoot()) {
          n++;
        }
      }
      Preconditions.checkState(n == snapshotsByNames.size(), "#n=" + n
          + ", snapshotsByNames.size()=" + snapshotsByNames.size());
      out.print(", #snapshot=");
      out.println(n);

      dumpTreeRecursively(out, prefix, new Iterable<SnapshotAndINode>() {
        @Override
        public Iterator<SnapshotAndINode> iterator() {
          return new Iterator<SnapshotAndINode>() {
            final Iterator<DirectoryDiff> i = getDiffs().iterator();
            private DirectoryDiff next = findNext();
  
            private DirectoryDiff findNext() {
              for(; i.hasNext(); ) {
                final DirectoryDiff diff = i.next();
                if (diff.isSnapshotRoot()) {
                  return diff;
                }
              }
              return null;
            }

            @Override
            public boolean hasNext() {
              return next != null;
            }
  
            @Override
            public SnapshotAndINode next() {
              final SnapshotAndINode pair = new SnapshotAndINode(next
                  .getSnapshotId(), getSnapshotById(next.getSnapshotId())
                  .getRoot());
              next = findNext();
              return pair;
            }
  
            @Override
            public void remove() {
              throw new UnsupportedOperationException();
            }
          };
        }
      });
    }
  }
}
