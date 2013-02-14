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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory.INodesInPath;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectorySnapshottable.SnapshotDiffInfo;

/**
 * Manage snapshottable directories and their snapshots.
 * 
 * This class includes operations that create, access, modify snapshots and/or
 * snapshot-related data. In general, the locking structure of snapshot
 * operations is: <br>
 * 
 * 1. Lock the {@link FSNamesystem} lock in {@link FSNamesystem} before calling
 * into {@link SnapshotManager} methods.<br>
 * 2. Lock the {@link FSDirectory} lock for the {@link SnapshotManager} methods
 * if necessary.
 */
public class SnapshotManager implements SnapshotStats {
  private final FSDirectory fsdir;

  private final AtomicInteger numSnapshottableDirs = new AtomicInteger();
  private final AtomicInteger numSnapshots = new AtomicInteger();

  private int snapshotCounter = 0;
  
  /** All snapshottable directories in the namesystem. */
  private final List<INodeDirectorySnapshottable> snapshottables
      = new ArrayList<INodeDirectorySnapshottable>();

  public SnapshotManager(final FSDirectory fsdir) {
    this.fsdir = fsdir;
  }

  /**
   * Set the given directory as a snapshottable directory.
   * If the path is already a snapshottable directory, update the quota.
   */
  public void setSnapshottable(final String path) throws IOException {
    final INodesInPath iip = fsdir.getLastINodeInPath(path);
    final INodeDirectory d = INodeDirectory.valueOf(iip.getINode(0), path);
    if (d.isSnapshottable()) {
      //The directory is already a snapshottable directory.
      ((INodeDirectorySnapshottable)d).setSnapshotQuota(
          INodeDirectorySnapshottable.SNAPSHOT_LIMIT);
      return;
    }

    final INodeDirectorySnapshottable s
        = d.replaceSelf4INodeDirectorySnapshottable(iip.getLatestSnapshot());
    snapshottables.add(s);
    numSnapshottableDirs.getAndIncrement();
  }

  /**
   * Set the given snapshottable directory to non-snapshottable.
   * 
   * @throws SnapshotException if there are snapshots in the directory.
   */
  public void resetSnapshottable(final String path
      ) throws IOException {
    final INodesInPath iip = fsdir.getLastINodeInPath(path);
    final INodeDirectorySnapshottable s = INodeDirectorySnapshottable.valueOf(
        iip.getINode(0), path);
    if (s.getNumSnapshots() > 0) {
      throw new SnapshotException("The directory " + path + " has snapshot(s). "
          + "Please redo the operation after removing all the snapshots.");
    }

    s.replaceSelf(iip.getLatestSnapshot());
    snapshottables.remove(s);

    numSnapshottableDirs.getAndDecrement();
  }

  /**
   * Create a snapshot of the given path.
   * @param path
   *          The directory path where the snapshot will be taken.
   * @param snapshotName
   *          The name of the snapshot.
   * @throws IOException
   *           Throw IOException when 1) the given path does not lead to an
   *           existing snapshottable directory, and/or 2) there exists a
   *           snapshot with the given name for the directory, and/or 3)
   *           snapshot number exceeds quota
   */
  public void createSnapshot(final String path, final String snapshotName
      ) throws IOException {
    // Find the source root directory path where the snapshot is taken.
    final INodesInPath i = fsdir.getINodesInPath4Write(path);
    final INodeDirectorySnapshottable srcRoot
        = INodeDirectorySnapshottable.valueOf(i.getLastINode(), path);
    srcRoot.addSnapshot(snapshotCounter, snapshotName);
      
    //create success, update id
    snapshotCounter++;
    numSnapshots.getAndIncrement();
  }
  
  /**
   * Delete a snapshot for a snapshottable directory
   * @param path Path to the directory where the snapshot was taken
   * @param snapshotName Name of the snapshot to be deleted
   * @param collectedBlocks Used to collect information to update blocksMap 
   * @throws IOException
   */
  public void deleteSnapshot(final String path, final String snapshotName,
      BlocksMapUpdateInfo collectedBlocks) throws IOException {
    // parse the path, and check if the path is a snapshot path
    INodesInPath inodesInPath = fsdir.getINodesInPath4Write(path.toString());
    // transfer the inode for path to an INodeDirectorySnapshottable.
    // the INodeDirectorySnapshottable#valueOf method will throw Exception 
    // if the path is not for a snapshottable directory
    INodeDirectorySnapshottable dir = INodeDirectorySnapshottable.valueOf(
        inodesInPath.getLastINode(), path.toString());
    
    dir.removeSnapshot(snapshotName, collectedBlocks);
    numSnapshots.getAndDecrement();
  }

  /**
   * Rename the given snapshot
   * @param path
   *          The directory path where the snapshot was taken
   * @param oldSnapshotName
   *          Old name of the snapshot
   * @param newSnapshotName
   *          New name of the snapshot
   * @throws IOException
   *           Throw IOException when 1) the given path does not lead to an
   *           existing snapshottable directory, and/or 2) the snapshot with the
   *           old name does not exist for the directory, and/or 3) there exists
   *           a snapshot with the new name for the directory
   */
  public void renameSnapshot(final String path, final String oldSnapshotName,
      final String newSnapshotName) throws IOException {
    // Find the source root directory path where the snapshot was taken.
    // All the check for path has been included in the valueOf method.
    final INodeDirectorySnapshottable srcRoot
        = INodeDirectorySnapshottable.valueOf(fsdir.getINode(path), path);
    // Note that renameSnapshot and createSnapshot are synchronized externally
    // through FSNamesystem's write lock
    srcRoot.renameSnapshot(path, oldSnapshotName, newSnapshotName);
  }
  
  @Override
  public long getNumSnapshottableDirs() {
    return numSnapshottableDirs.get();
  }

  @Override
  public long getNumSnapshots() {
    return numSnapshots.get();
  }
  
  /**
   * Write {@link #snapshotCounter}, {@link #numSnapshots}, and
   * {@link #numSnapshottableDirs} to the DataOutput.
   */
  public void write(DataOutput out) throws IOException {
    out.writeInt(snapshotCounter);
    out.writeInt(numSnapshots.get());
    out.writeInt(numSnapshottableDirs.get());
  }
  
  /**
   * Read values of {@link #snapshotCounter}, {@link #numSnapshots}, and
   * {@link #numSnapshottableDirs} from the DataInput
   */
  public void read(DataInput in) throws IOException {
    snapshotCounter = in.readInt();
    numSnapshots.set(in.readInt());
    numSnapshottableDirs.set(in.readInt());
  }
  
  /**
   * List all the snapshottable directories that are owned by the current user.
   * @param userName Current user name.
   * @return Snapshottable directories that are owned by the current user,
   *         represented as an array of {@link SnapshottableDirectoryStatus}. If
   *         {@code userName} is null, return all the snapshottable dirs.
   */
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing(
      String userName) {
    if (snapshottables.isEmpty()) {
      return null;
    }
    
    List<SnapshottableDirectoryStatus> statusList = 
        new ArrayList<SnapshottableDirectoryStatus>();
    for (INodeDirectorySnapshottable dir : snapshottables) {
      if (userName == null || userName.equals(dir.getUserName())) {
        SnapshottableDirectoryStatus status = new SnapshottableDirectoryStatus(
            dir.getModificationTime(), dir.getAccessTime(),
            dir.getFsPermission(), dir.getUserName(), dir.getGroupName(),
            dir.getLocalNameBytes(), dir.getId(), dir.getNumSnapshots(),
            dir.getSnapshotQuota(), dir.getParent() == null ? INode.EMPTY_BYTES
                : DFSUtil.string2Bytes(dir.getParent().getFullPathName()));
        statusList.add(status);
      }
    }
    return statusList.toArray(new SnapshottableDirectoryStatus[statusList
        .size()]);
  }
  
  /**
   * Remove snapshottable directories from {@link #snapshottables}
   * @param toRemoveList A list of INodeDirectorySnapshottable to be removed
   */
  public void removeSnapshottableDirs(
      List<INodeDirectorySnapshottable> toRemoveList) {
    if (toRemoveList != null) {
      this.snapshottables.removeAll(toRemoveList);
    }
  }
  
  /**
   * Compute the difference between two snapshots of a directory, or between a
   * snapshot of the directory and its current tree.
   */
  public SnapshotDiffInfo diff(final String path, final String from,
      final String to) throws IOException {
    if ((from == null || from.isEmpty())
        && (to == null || to.isEmpty())) {
      // both fromSnapshot and toSnapshot indicate the current tree
      return null;
    }
    // if the start point is equal to the end point, return null
    if (from.equals(to)) {
      return null;
    }

    // Find the source root directory path where the snapshots were taken.
    // All the check for path has been included in the valueOf method.
    INodesInPath inodesInPath = fsdir.getINodesInPath4Write(path.toString());
    final INodeDirectorySnapshottable snapshotRoot = INodeDirectorySnapshottable
        .valueOf(inodesInPath.getLastINode(), path);
    
    return snapshotRoot.computeDiff(from, to);
  }
 
}