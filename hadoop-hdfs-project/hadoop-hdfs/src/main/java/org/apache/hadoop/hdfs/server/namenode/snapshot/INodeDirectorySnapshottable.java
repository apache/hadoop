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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.snapshot.diff.Diff;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.util.Time;

import com.google.common.base.Preconditions;

/**
 * Directories where taking snapshots is allowed.
 * 
 * Like other {@link INode} subclasses, this class is synchronized externally
 * by the namesystem and FSDirectory locks.
 */
@InterfaceAudience.Private
public class INodeDirectorySnapshottable extends INodeDirectoryWithSnapshot {
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
  public static class SnapshotDiffReport {
    public static final Comparator<INode> INODE_COMPARATOR = 
        new Comparator<INode>() {
      @Override
      public int compare(INode left, INode right) {
        if (left == null) {
          return right == null ? 0 : -1;
        } else {
          return right == null ? 1 : left.compareTo(right.getLocalNameBytes());
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
     * A map capturing the detailed difference. Each key indicates a directory
     * whose metadata or children have been changed between the two snapshots,
     * while its associated value is a {@link Diff} storing the changes happened
     * to the children (files).
     */
    private final SortedMap<INodeDirectoryWithSnapshot, ChildrenDiff> diffMap;
    
    public SnapshotDiffReport(INodeDirectorySnapshottable snapshotRoot,
        Snapshot start, Snapshot end) {
      this.snapshotRoot = snapshotRoot;
      this.from = start;
      this.to = end;
      this.diffMap = new TreeMap<INodeDirectoryWithSnapshot, ChildrenDiff>(
          INODE_COMPARATOR);
    }
    
    /** Add a dir-diff pair into {@link #diffMap} */
    public void addDiff(INodeDirectoryWithSnapshot dir, ChildrenDiff diff) {
      diffMap.put(dir, diff);
    }
    
    /**
     * dump the diff
     */
    public String dump() {
      StringBuilder strBuffer = new StringBuilder();
      String fromStr = from == null ? "current directory" : "snapshot "
          + from.getRoot().getLocalName();
      String toStr = to == null ? "current directory" : "snapshot "
          + to.getRoot().getLocalName();
      strBuffer.append("Diffence between snapshot " + fromStr + " and " + toStr
          + " under directory " + snapshotRoot.getFullPathName() + ":\n");
      
      if (!diffMap.isEmpty()) {
        for (Map.Entry<INodeDirectoryWithSnapshot, ChildrenDiff> entry : diffMap
            .entrySet()) {
          strBuffer.append("M\t" + entry.getKey().getFullPathName() + "\n");
          entry.getValue().printDiff(strBuffer, entry.getKey(),
              from == null || 
              (to != null && Snapshot.ID_COMPARATOR.compare(from, to) > 0));
        }
      }
      return strBuffer.toString();
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
    super(dir, true, dir instanceof INodeDirectoryWithSnapshot ? 
        ((INodeDirectoryWithSnapshot) dir).getSnapshotDiffs() : null);
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
      int indexOfNew = searchSnapshot(DFSUtil.string2Bytes(newName));
      if (indexOfNew > 0) {
        throw new SnapshotException("The snapshot " + newName
            + " already exists for directory " + path);
      }
      // remove the one with old name from snapshotsByNames
      Snapshot snapshot = snapshotsByNames.remove(indexOfOld);
      final INodeDirectory ssRoot = snapshot.getRoot();
      ssRoot.setLocalName(newName);
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
  Snapshot addSnapshot(int id, String name) throws SnapshotException {
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
          + "snapshot with the same name \"" + name + "\".");
    }

    addSnapshotDiff(s, this, true);
    snapshotsByNames.add(-i - 1, s);

    //set modification time
    final long timestamp = Time.now();
    s.getRoot().updateModificationTime(timestamp, null);
    updateModificationTime(timestamp, null);
    return s;
  }
  
  /**
   * Remove the snapshot with the given name from {@link #snapshotsByNames},
   * and delete all the corresponding SnapshotDiff.
   * 
   * @param snapshotName The name of the snapshot to be removed
   * @param collectedBlocks Used to collect information to update blocksMap
   * @return The removed snapshot. Null if no snapshot with the given name 
   *         exists.
   */
  Snapshot removeSnapshot(String snapshotName,
      BlocksMapUpdateInfo collectedBlocks) throws SnapshotException {
    final int indexOfOld = searchSnapshot(DFSUtil.string2Bytes(snapshotName));
    if (indexOfOld < 0) {
      throw new SnapshotException("Cannot delete snapshot " + snapshotName
          + " from path " + this.getFullPathName()
          + ": the snapshot does not exist.");
    } else {
      Snapshot snapshot = snapshotsByNames.remove(indexOfOld);
      deleteDiffsForSnapshot(snapshot, this, collectedBlocks);
      return snapshot;
    }
  }
  
  /**
   * Recursively delete SnapshotDiff associated with the given snapshot under a
   * directory
   */
  private void deleteDiffsForSnapshot(Snapshot snapshot, INodeDirectory dir,
      BlocksMapUpdateInfo collectedBlocks) {
    if (dir instanceof INodeDirectoryWithSnapshot) {
      INodeDirectoryWithSnapshot sdir = (INodeDirectoryWithSnapshot) dir;
      sdir.deleteSnapshotDiff(snapshot, collectedBlocks);
    }
    ReadOnlyList<INode> children = dir.getChildrenList(null);
    for (INode child : children) {
      if (child instanceof INodeDirectory) {
        deleteDiffsForSnapshot(snapshot, (INodeDirectory) child,
            collectedBlocks);
      }
    }
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
  SnapshotDiffReport computeDiff(final String from, final String to)
      throws SnapshotException {
    Snapshot fromSnapshot = getSnapshotByName(from);
    Snapshot toSnapshot = getSnapshotByName(to); 
    SnapshotDiffReport diffs = new SnapshotDiffReport(this, fromSnapshot,
        toSnapshot);
    computeDiffInDir(this, diffs);
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
   * directory.
   * @param dir The directory under which the diff is computed.
   * @param diffReport data structure used to store the diff.
   */
  private void computeDiffInDir(INodeDirectory dir,
      SnapshotDiffReport diffReport) {
    ChildrenDiff diff = new ChildrenDiff();
    if (dir instanceof INodeDirectoryWithSnapshot) {
      boolean change = ((INodeDirectoryWithSnapshot) dir)
          .computeDiffBetweenSnapshots(diffReport.from,
              diffReport.to, diff);
      if (change) {
        diffReport.addDiff((INodeDirectoryWithSnapshot) dir,
            diff); 
      }
    }
    ReadOnlyList<INode> children = dir.getChildrenList(null);
    for (INode child : children) {
      if (child instanceof INodeDirectory
          && diff.searchCreated(child.getLocalNameBytes()) == null) {
        // Compute diff recursively for children that are directories. We do not
        // need to compute diff for those contained in the created list since 
        // directory contained in the created list must be new created.
        computeDiffInDir((INodeDirectory) child, diffReport);
      }
    }
  }
  
  /**
   * Replace itself with {@link INodeDirectoryWithSnapshot} or
   * {@link INodeDirectory} depending on the latest snapshot.
   */
  void replaceSelf(final Snapshot latest) {
    if (latest == null) {
      Preconditions.checkState(getLastSnapshot() == null,
          "latest == null but getLastSnapshot() != null, this=%s", this);
      replaceSelf4INodeDirectory();
    } else {
      replaceSelf4INodeDirectoryWithSnapshot(latest).recordModification(latest);
    }
  }

  @Override
  public void dumpTreeRecursively(PrintWriter out, StringBuilder prefix,
      Snapshot snapshot) {
    super.dumpTreeRecursively(out, prefix, snapshot);

    if (snapshot == null) {
      out.println();
      out.print(prefix);
      int n = 0;
      for(SnapshotDiff diff : getSnapshotDiffs()) {
        if (diff.isSnapshotRoot()) {
          n++;
        }
      }
      out.print(n);
      out.print(n <= 1 ? " snapshot of " : " snapshots of ");
      final String name = getLocalName();
      out.println(name.isEmpty()? "/": name);

      dumpTreeRecursively(out, prefix, new Iterable<Pair<? extends INode, Snapshot>>() {
        @Override
        public Iterator<Pair<? extends INode, Snapshot>> iterator() {
          return new Iterator<Pair<? extends INode, Snapshot>>() {
            final Iterator<SnapshotDiff> i = getSnapshotDiffs().iterator();
            private SnapshotDiff next = findNext();
  
            private SnapshotDiff findNext() {
              for(; i.hasNext(); ) {
                final SnapshotDiff diff = i.next();
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
            public Pair<INodeDirectory, Snapshot> next() {
              final Snapshot s = next.snapshot;
              final Pair<INodeDirectory, Snapshot> pair =
                  new Pair<INodeDirectory, Snapshot>(s.getRoot(), s);
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
