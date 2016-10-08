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

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.namenode.Content;
import org.apache.hadoop.hdfs.server.namenode.ContentCounts;
import org.apache.hadoop.hdfs.server.namenode.ContentSummaryComputationContext;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory.SnapshotAndINode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithCount;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithName;
import org.apache.hadoop.hdfs.util.Diff.ListType;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A directory with this feature is a snapshottable directory, where snapshots
 * can be taken. This feature extends {@link DirectoryWithSnapshotFeature}, and
 * maintains extra information about all the snapshots taken on this directory.
 */
@InterfaceAudience.Private
public class DirectorySnapshottableFeature extends DirectoryWithSnapshotFeature {
  /** Limit the number of snapshot per snapshottable directory. */
  static final int SNAPSHOT_LIMIT = 1 << 16;

  /**
   * Snapshots of this directory in ascending order of snapshot names.
   * Note that snapshots in ascending order of snapshot id are stored in
   * {@link DirectoryWithSnapshotFeature}.diffs (a private field).
   */
  private final List<Snapshot> snapshotsByNames = new ArrayList<Snapshot>();
  /** Number of snapshots allowed. */
  private int snapshotQuota = SNAPSHOT_LIMIT;

  public DirectorySnapshottableFeature(DirectoryWithSnapshotFeature feature) {
    super(feature == null ? null : feature.getDiffs());
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

  public Snapshot getSnapshotById(int sid) {
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
      if (indexOfNew >= 0) {
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

  /**
   * Simply add a snapshot into the {@link #snapshotsByNames}. Used when loading
   * fsimage.
   */
  void addSnapshot(Snapshot snapshot) {
    this.snapshotsByNames.add(snapshot);
  }

  /** Add a snapshot. */
  public Snapshot addSnapshot(INodeDirectory snapshotRoot, int id, String name)
      throws SnapshotException, QuotaExceededException {
    //check snapshot quota
    final int n = getNumSnapshots();
    if (n + 1 > snapshotQuota) {
      throw new SnapshotException("Failed to add snapshot: there are already "
          + n + " snapshot(s) and the snapshot quota is "
          + snapshotQuota);
    }
    final Snapshot s = new Snapshot(id, name, snapshotRoot);
    final byte[] nameBytes = s.getRoot().getLocalNameBytes();
    final int i = searchSnapshot(nameBytes);
    if (i >= 0) {
      throw new SnapshotException("Failed to add snapshot: there is already a "
          + "snapshot with the same name \"" + Snapshot.getSnapshotName(s) + "\".");
    }

    final DirectoryDiff d = getDiffs().addDiff(id, snapshotRoot);
    d.setSnapshotRoot(s.getRoot());
    snapshotsByNames.add(-i - 1, s);

    // set modification time
    final long now = Time.now();
    snapshotRoot.updateModificationTime(now, Snapshot.CURRENT_STATE_ID);
    s.getRoot().setModificationTime(now, Snapshot.CURRENT_STATE_ID);
    return s;
  }

  /**
   * Remove the snapshot with the given name from {@link #snapshotsByNames},
   * and delete all the corresponding DirectoryDiff.
   *
   * @param reclaimContext records blocks and inodes that need to be reclaimed
   * @param snapshotRoot The directory where we take snapshots
   * @param snapshotName The name of the snapshot to be removed
   * @return The removed snapshot. Null if no snapshot with the given name
   *         exists.
   */
  public Snapshot removeSnapshot(
      INode.ReclaimContext reclaimContext, INodeDirectory snapshotRoot,
      String snapshotName) throws SnapshotException {
    final int i = searchSnapshot(DFSUtil.string2Bytes(snapshotName));
    if (i < 0) {
      throw new SnapshotException("Cannot delete snapshot " + snapshotName
          + " from path " + snapshotRoot.getFullPathName()
          + ": the snapshot does not exist.");
    } else {
      final Snapshot snapshot = snapshotsByNames.get(i);
      int prior = Snapshot.findLatestSnapshot(snapshotRoot, snapshot.getId());
      snapshotRoot.cleanSubtree(reclaimContext, snapshot.getId(), prior);
      // remove from snapshotsByNames after successfully cleaning the subtree
      snapshotsByNames.remove(i);
      return snapshot;
    }
  }

  @Override
  public void computeContentSummary4Snapshot(ContentSummaryComputationContext
                                                   context) {
    ContentCounts counts = context.getCounts();
    counts.addContent(Content.SNAPSHOT, snapshotsByNames.size());
    counts.addContent(Content.SNAPSHOTTABLE_DIRECTORY, 1);
    super.computeContentSummary4Snapshot(context);
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
  SnapshotDiffInfo computeDiff(final INodeDirectory snapshotRoot,
      final String from, final String to) throws SnapshotException {
    Snapshot fromSnapshot = getSnapshotByName(snapshotRoot, from);
    Snapshot toSnapshot = getSnapshotByName(snapshotRoot, to);
    // if the start point is equal to the end point, return null
    if (from.equals(to)) {
      return null;
    }
    SnapshotDiffInfo diffs = new SnapshotDiffInfo(snapshotRoot, fromSnapshot,
        toSnapshot);
    computeDiffRecursively(snapshotRoot, snapshotRoot, new ArrayList<byte[]>(),
        diffs);
    return diffs;
  }

  /**
   * Find the snapshot matching the given name.
   *
   * @param snapshotRoot The directory where snapshots were taken.
   * @param snapshotName The name of the snapshot.
   * @return The corresponding snapshot. Null if snapshotName is null or empty.
   * @throws SnapshotException If snapshotName is not null or empty, but there
   *           is no snapshot matching the name.
   */
  private Snapshot getSnapshotByName(INodeDirectory snapshotRoot,
      String snapshotName) throws SnapshotException {
    Snapshot s = null;
    if (snapshotName != null && !snapshotName.isEmpty()) {
      final int index = searchSnapshot(DFSUtil.string2Bytes(snapshotName));
      if (index < 0) {
        throw new SnapshotException("Cannot find the snapshot of directory "
            + snapshotRoot.getFullPathName() + " with name " + snapshotName);
      }
      s = snapshotsByNames.get(index);
    }
    return s;
  }

  /**
   * Recursively compute the difference between snapshots under a given
   * directory/file.
   * @param snapshotRoot The directory where snapshots were taken.
   * @param node The directory/file under which the diff is computed.
   * @param parentPath Relative path (corresponding to the snapshot root) of
   *                   the node's parent.
   * @param diffReport data structure used to store the diff.
   */
  private void computeDiffRecursively(final INodeDirectory snapshotRoot,
      INode node, List<byte[]> parentPath, SnapshotDiffInfo diffReport) {
    final Snapshot earlierSnapshot = diffReport.isFromEarlier() ?
        diffReport.getFrom() : diffReport.getTo();
    final Snapshot laterSnapshot = diffReport.isFromEarlier() ?
        diffReport.getTo() : diffReport.getFrom();
    byte[][] relativePath = parentPath.toArray(new byte[parentPath.size()][]);
    if (node.isDirectory()) {
      final ChildrenDiff diff = new ChildrenDiff();
      INodeDirectory dir = node.asDirectory();
      DirectoryWithSnapshotFeature sf = dir.getDirectoryWithSnapshotFeature();
      if (sf != null) {
        boolean change = sf.computeDiffBetweenSnapshots(earlierSnapshot,
            laterSnapshot, diff, dir);
        if (change) {
          diffReport.addDirDiff(dir, relativePath, diff);
        }
      }
      ReadOnlyList<INode> children = dir.getChildrenList(earlierSnapshot
          .getId());
      for (INode child : children) {
        final byte[] name = child.getLocalNameBytes();
        boolean toProcess = diff.searchIndex(ListType.DELETED, name) < 0;
        if (!toProcess && child instanceof INodeReference.WithName) {
          byte[][] renameTargetPath = findRenameTargetPath(
              snapshotRoot, (WithName) child,
              laterSnapshot == null ? Snapshot.CURRENT_STATE_ID :
                laterSnapshot.getId());
          if (renameTargetPath != null) {
            toProcess = true;
            diffReport.setRenameTarget(child.getId(), renameTargetPath);
          }
        }
        if (toProcess) {
          parentPath.add(name);
          computeDiffRecursively(snapshotRoot, child, parentPath, diffReport);
          parentPath.remove(parentPath.size() - 1);
        }
      }
    } else if (node.isFile() && node.asFile().isWithSnapshot()) {
      INodeFile file = node.asFile();
      boolean change = file.getFileWithSnapshotFeature()
          .changedBetweenSnapshots(file, earlierSnapshot, laterSnapshot);
      if (change) {
        diffReport.addFileDiff(file, relativePath);
      }
    }
  }

  /**
   * We just found a deleted WithName node as the source of a rename operation.
   * However, we should include it in our snapshot diff report as rename only
   * if the rename target is also under the same snapshottable directory.
   */
  private byte[][] findRenameTargetPath(final INodeDirectory snapshotRoot,
      INodeReference.WithName wn, final int snapshotId) {
    INode inode = wn.getReferredINode();
    final LinkedList<byte[]> ancestors = Lists.newLinkedList();
    while (inode != null) {
      if (inode == snapshotRoot) {
        return ancestors.toArray(new byte[ancestors.size()][]);
      }
      if (inode instanceof INodeReference.WithCount) {
        inode = ((WithCount) inode).getParentRef(snapshotId);
      } else {
        INode parent = inode.getParentReference() != null ? inode
            .getParentReference() : inode.getParent();
        if (parent != null && parent instanceof INodeDirectory) {
          int sid = parent.asDirectory().searchChild(inode);
          if (sid < snapshotId) {
            return null;
          }
        }
        if (!(parent instanceof WithCount)) {
          ancestors.addFirst(inode.getLocalNameBytes());
        }
        inode = parent;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return "snapshotsByNames=" + snapshotsByNames;
  }

  @VisibleForTesting
  public void dumpTreeRecursively(INodeDirectory snapshotRoot, PrintWriter out,
      StringBuilder prefix, int snapshot) {
    if (snapshot == Snapshot.CURRENT_STATE_ID) {
      out.println();
      out.print(prefix);

      out.print("Snapshot of ");
      final String name = snapshotRoot.getLocalName();
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

      INodeDirectory.dumpTreeRecursively(out, prefix,
          new Iterable<SnapshotAndINode>() {
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
