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
import java.util.Set;
import java.util.Arrays;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.Content;
import org.apache.hadoop.hdfs.server.namenode.ContentCounts;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory.SnapshotAndINode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithCount;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithName;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.security.AccessControlException;
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
  static final int SNAPSHOT_QUOTA_DEFAULT = 1 << 16;

  /**
   * Snapshots of this directory in ascending order of snapshot names.
   * Note that snapshots in ascending order of snapshot id are stored in
   * {@link DirectoryWithSnapshotFeature}.diffs (a private field).
   */
  private final List<Snapshot> snapshotsByNames = new ArrayList<Snapshot>();
  /** Number of snapshots allowed. */
  private int snapshotQuota = SNAPSHOT_QUOTA_DEFAULT;

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
    final int indexOfOld = searchSnapshot(DFSUtil.string2Bytes(oldName));
    if (indexOfOld < 0) {
      throw new SnapshotException("The snapshot " + oldName
          + " does not exist for directory " + path);
    } else {
      if (newName.equals(oldName)) {
        return;
      }
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
  public Snapshot addSnapshot(INodeDirectory snapshotRoot, int id, String name,
      final LeaseManager leaseManager, final boolean captureOpenFiles,
      int maxSnapshotLimit)
      throws SnapshotException {
    //check snapshot quota
    final int n = getNumSnapshots();
    if (n + 1 > snapshotQuota) {
      throw new SnapshotException("Failed to add snapshot: there are already "
          + n + " snapshot(s) and the snapshot quota is "
          + snapshotQuota);
    } else if (n + 1 > maxSnapshotLimit) {
      throw new SnapshotException(
          "Failed to add snapshot: there are already " + n
              + " snapshot(s) and the max snapshot limit is "
              + maxSnapshotLimit);
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

    if (captureOpenFiles) {
      try {
        Set<INodesInPath> openFilesIIP =
            leaseManager.getINodeWithLeases(snapshotRoot);
        for (INodesInPath openFileIIP : openFilesIIP) {
          INodeFile openFile = openFileIIP.getLastINode().asFile();
          openFile.recordModification(openFileIIP.getLatestSnapshotId());
        }
      } catch (Exception e) {
        throw new SnapshotException("Failed to add snapshot: Unable to " +
            "capture all open files under the snapshot dir " +
            snapshotRoot.getFullPathName() + " for snapshot '" + name + "'", e);
      }
    }
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
  public void computeContentSummary4Snapshot(final BlockStoragePolicySuite bsps,
      final ContentCounts counts) throws AccessControlException {
    counts.addContent(Content.SNAPSHOT, snapshotsByNames.size());
    counts.addContent(Content.SNAPSHOTTABLE_DIRECTORY, 1);
    super.computeContentSummary4Snapshot(bsps, counts);
  }

  /**
   * Compute the difference between two snapshots (or a snapshot and the current
   * directory) of the directory. The diff calculation can be scoped to either
   * the snapshot root or any descendant directory under the snapshot root.
   *
   * @param snapshotRootDir the snapshot root directory
   * @param snapshotDiffScopeDir the descendant directory under snapshot root
   *          to scope the diff calculation to.
   * @param from The name of the start point of the comparison. Null indicating
   *          the current tree.
   * @param to The name of the end point. Null indicating the current tree.
   * @return The difference between the start/end points.
   * @throws SnapshotException If there is no snapshot matching the starting
   *           point, or if endSnapshotName is not null but cannot be identified
   *           as a previous snapshot.
   */
  SnapshotDiffInfo computeDiff(final INodeDirectory snapshotRootDir,
      final INodeDirectory snapshotDiffScopeDir, final String from,
      final String to) throws SnapshotException {
    Preconditions.checkArgument(snapshotDiffScopeDir
        .isDescendantOfSnapshotRoot(snapshotRootDir));
    Snapshot fromSnapshot = getSnapshotByName(snapshotRootDir, from);
    Snapshot toSnapshot = getSnapshotByName(snapshotRootDir, to);
    // if the start point is equal to the end point, return null
    if (from != null && from.equals(to)) {
      return null;
    }
    SnapshotDiffInfo diffs = new SnapshotDiffInfo(snapshotRootDir,
        snapshotDiffScopeDir, fromSnapshot, toSnapshot);
    // The snapshot diff scope dir is passed in as the snapshot dir
    // so that the file paths in the diff report are relative to the
    // snapshot scope dir.
    computeDiffRecursively(snapshotDiffScopeDir, snapshotDiffScopeDir,
        new ArrayList<>(), diffs);
    return diffs;
  }

  /**
   * Compute the difference between two snapshots (or a snapshot and the current
   * directory) of the directory. The diff calculation can be scoped to either
   * the snapshot root or any descendant directory under the snapshot root.
   *
   * @param snapshotRootDir the snapshot root directory
   * @param snapshotDiffScopeDir the descendant directory under snapshot root
   *          to scope the diff calculation to.
   * @param from The name of the start point of the comparison. Null indicating
   *          the current tree.
   * @param to The name of the end point. Null indicating the current tree.
   * @param startPath
   *           path relative to the snapshottable root directory from where the
   *           snapshotdiff computation needs to start across multiple rpc calls
   * @param index
   *           index in the created or deleted list of the directory at which
   *           the snapshotdiff computation stopped during the last rpc call
   *           as the no of entries exceeded the snapshotdiffentry limit. -1
   *           indicates, the snapshotdiff computation needs to start right
   *           from the startPath provided.
   *
   * @return The difference between the start/end points.
   * @throws SnapshotException If there is no snapshot matching the starting
   *           point, or if endSnapshotName is not null but cannot be identified
   *           as a previous snapshot.
   */
  SnapshotDiffListingInfo computeDiff(final INodeDirectory snapshotRootDir,
      final INodeDirectory snapshotDiffScopeDir, final String from,
      final String to, byte[] startPath, int index,
      int snapshotDiffReportEntriesLimit) throws SnapshotException {
    Preconditions.checkArgument(
        snapshotDiffScopeDir.isDescendantOfSnapshotRoot(snapshotRootDir));
    Snapshot fromSnapshot = getSnapshotByName(snapshotRootDir, from);
    Snapshot toSnapshot = getSnapshotByName(snapshotRootDir, to);
    boolean toProcess = Arrays.equals(startPath, DFSUtilClient.EMPTY_BYTES);
    byte[][] resumePath = DFSUtilClient.bytes2byteArray(startPath);
    if (from.equals(to)) {
      return null;
    }
    SnapshotDiffListingInfo diffs =
        new SnapshotDiffListingInfo(snapshotRootDir, snapshotDiffScopeDir,
            fromSnapshot, toSnapshot, snapshotDiffReportEntriesLimit);
    diffs.setLastIndex(index);
    computeDiffRecursively(snapshotDiffScopeDir, snapshotDiffScopeDir,
        new ArrayList<byte[]>(), diffs, resumePath, 0, toProcess);
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
   * @param snapshotDir The directory where snapshots were taken. Can be a
   *                    snapshot root directory or any descendant directory
   *                    under snapshot root directory.
   * @param node The directory/file under which the diff is computed.
   * @param parentPath Relative path (corresponding to the snapshot root) of
   *                   the node's parent.
   * @param diffReport data structure used to store the diff.
   */
  private void computeDiffRecursively(final INodeDirectory snapshotDir,
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
        boolean toProcess = !diff.containsDeleted(name);
        if (!toProcess && child instanceof INodeReference.WithName) {
          byte[][] renameTargetPath = findRenameTargetPath(
              snapshotDir, (WithName) child,
              laterSnapshot == null ? Snapshot.CURRENT_STATE_ID :
                laterSnapshot.getId());
          if (renameTargetPath != null) {
            toProcess = true;
            diffReport.setRenameTarget(child.getId(), renameTargetPath);
          }
        }
        if (toProcess) {
          parentPath.add(name);
          computeDiffRecursively(snapshotDir, child, parentPath, diffReport);
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
   * Recursively compute the difference between snapshots under a given
   * directory/file partially.
   * @param snapshotDir The directory where snapshots were taken. Can be a
   *                    snapshot root directory or any descendant directory
   *                    under snapshot root directory.
   * @param node The directory/file under which the diff is computed.
   * @param parentPath Relative path (corresponding to the snapshot root) of
   *                   the node's parent.
   * @param diffReport data structure used to store the diff.
   * @param resume  path from where to resume the snapshotdiff computation
   *                    in one rpc call
   * @param level       indicates the level of the directory tree rooted at
   *                    snapshotRoot.
   * @param processFlag indicates that the dir/file where the snapshotdiff
   *                    computation has to start is processed or not.
   */
  private boolean computeDiffRecursively(final INodeDirectory snapshotDir,
       INode node, List<byte[]> parentPath, SnapshotDiffListingInfo diffReport,
       final byte[][] resume, int level, boolean processFlag) {
    final Snapshot earlier = diffReport.getEarlier();
    final Snapshot later = diffReport.getLater();
    byte[][] relativePath = parentPath.toArray(new byte[parentPath.size()][]);
    if (!processFlag && level == resume.length
        && Arrays.equals(resume[resume.length - 1], node.getLocalNameBytes())) {
      processFlag = true;
    }

    if (node.isDirectory()) {
      final ChildrenDiff diff = new ChildrenDiff();
      INodeDirectory dir = node.asDirectory();
      if (processFlag) {
        DirectoryWithSnapshotFeature sf = dir.getDirectoryWithSnapshotFeature();
        if (sf != null) {
          boolean change =
              sf.computeDiffBetweenSnapshots(earlier, later, diff, dir);
          if (change) {
            if (!diffReport.addDirDiff(dir.getId(), relativePath, diff)) {
              return false;
            }
          }
        }
      }

      ReadOnlyList<INode> children = dir.getChildrenList(earlier.getId());
      boolean iterate = false;
      for (INode child : children) {
        final byte[] name = child.getLocalNameBytes();
        if (!processFlag && !iterate && !Arrays.equals(resume[level], name)) {
          continue;
        }
        iterate = true;
        level = level + 1;
        boolean toProcess = !diff.containsDeleted(name);
        if (!toProcess && child instanceof INodeReference.WithName) {
          byte[][] renameTargetPath = findRenameTargetPath(snapshotDir,
              (WithName) child, Snapshot.getSnapshotId(later));
          if (renameTargetPath != null) {
            toProcess = true;
          }
        }
        if (toProcess) {
          parentPath.add(name);
          processFlag = computeDiffRecursively(snapshotDir, child, parentPath,
              diffReport, resume, level, processFlag);
          parentPath.remove(parentPath.size() - 1);
          if (!processFlag) {
            return false;
          }
        }
      }
    } else if (node.isFile() && node.asFile().isWithSnapshot() && processFlag) {
      INodeFile file = node.asFile();
      boolean change = file.getFileWithSnapshotFeature()
          .changedBetweenSnapshots(file, earlier, later);
      if (change) {
        if (!diffReport.addFileDiff(file, relativePath)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * We just found a deleted WithName node as the source of a rename operation.
   * However, we should include it in our snapshot diff report as rename only
   * if the rename target is also under the same snapshottable directory.
   */
  public byte[][] findRenameTargetPath(final INodeDirectory snapshotRoot,
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
