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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ObjectName;

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.protocol.SnapshotInfo;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormat;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.apache.hadoop.metrics2.util.MBeans;

import com.google.common.base.Preconditions;

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
public class SnapshotManager implements SnapshotStatsMXBean {
  private boolean allowNestedSnapshots = false;
  private final FSDirectory fsdir;
  private static final int SNAPSHOT_ID_BIT_WIDTH = 24;

  private final AtomicInteger numSnapshots = new AtomicInteger();

  private int snapshotCounter = 0;
  
  /** All snapshottable directories in the namesystem. */
  private final Map<Long, INodeDirectory> snapshottables =
      new HashMap<Long, INodeDirectory>();

  public SnapshotManager(final FSDirectory fsdir) {
    this.fsdir = fsdir;
  }

  /** Used in tests only */
  void setAllowNestedSnapshots(boolean allowNestedSnapshots) {
    this.allowNestedSnapshots = allowNestedSnapshots;
  }

  private void checkNestedSnapshottable(INodeDirectory dir, String path)
      throws SnapshotException {
    if (allowNestedSnapshots) {
      return;
    }

    for(INodeDirectory s : snapshottables.values()) {
      if (s.isAncestorDirectory(dir)) {
        throw new SnapshotException(
            "Nested snapshottable directories not allowed: path=" + path
            + ", the subdirectory " + s.getFullPathName()
            + " is already a snapshottable directory.");
      }
      if (dir.isAncestorDirectory(s)) {
        throw new SnapshotException(
            "Nested snapshottable directories not allowed: path=" + path
            + ", the ancestor " + s.getFullPathName()
            + " is already a snapshottable directory.");
      }
    }
  }

  /**
   * Set the given directory as a snapshottable directory.
   * If the path is already a snapshottable directory, update the quota.
   */
  public void setSnapshottable(final String path, boolean checkNestedSnapshottable)
      throws IOException {
    final INodesInPath iip = fsdir.getINodesInPath4Write(path);
    final INodeDirectory d = INodeDirectory.valueOf(iip.getLastINode(), path);
    if (checkNestedSnapshottable) {
      checkNestedSnapshottable(d, path);
    }

    if (d.isSnapshottable()) {
      //The directory is already a snapshottable directory.
      d.setSnapshotQuota(DirectorySnapshottableFeature.SNAPSHOT_LIMIT);
    } else {
      d.addSnapshottableFeature();
    }
    addSnapshottable(d);
  }
  
  /** Add the given snapshottable directory to {@link #snapshottables}. */
  public void addSnapshottable(INodeDirectory dir) {
    Preconditions.checkArgument(dir.isSnapshottable());
    snapshottables.put(dir.getId(), dir);
  }

  /** Remove the given snapshottable directory from {@link #snapshottables}. */
  private void removeSnapshottable(INodeDirectory s) {
    snapshottables.remove(s.getId());
  }
  
  /** Remove snapshottable directories from {@link #snapshottables} */
  public void removeSnapshottable(List<INodeDirectory> toRemove) {
    if (toRemove != null) {
      for (INodeDirectory s : toRemove) {
        removeSnapshottable(s);
      }
    }
  }

  /**
   * Set the given snapshottable directory to non-snapshottable.
   * 
   * @throws SnapshotException if there are snapshots in the directory.
   */
  public void resetSnapshottable(final String path) throws IOException {
    final INodesInPath iip = fsdir.getINodesInPath4Write(path);
    final INodeDirectory d = INodeDirectory.valueOf(iip.getLastINode(), path);
    DirectorySnapshottableFeature sf = d.getDirectorySnapshottableFeature();
    if (sf == null) {
      // the directory is already non-snapshottable
      return;
    }
    if (sf.getNumSnapshots() > 0) {
      throw new SnapshotException("The directory " + path + " has snapshot(s). "
          + "Please redo the operation after removing all the snapshots.");
    }

    if (d == fsdir.getRoot()) {
      d.setSnapshotQuota(0);
    } else {
      d.removeSnapshottableFeature();
    }
    removeSnapshottable(d);
  }

  /**
  * Find the source root directory where the snapshot will be taken
  * for a given path.
  *
  * @param path The directory path where the snapshot will be taken.
  * @return Snapshottable directory.
  * @throws IOException
  *           Throw IOException when the given path does not lead to an
  *           existing snapshottable directory.
  */
  public INodeDirectory getSnapshottableRoot(final String path)
      throws IOException {
    final INodeDirectory dir = INodeDirectory.valueOf(fsdir
        .getINodesInPath4Write(path).getLastINode(), path);
    if (!dir.isSnapshottable()) {
      throw new SnapshotException(
          "Directory is not a snapshottable directory: " + path);
    }
    return dir;
  }

  /**
   * Create a snapshot of the given path.
   * It is assumed that the caller will perform synchronization.
   *
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
  public String createSnapshot(final String path, String snapshotName
      ) throws IOException {
    INodeDirectory srcRoot = getSnapshottableRoot(path);

    if (snapshotCounter == getMaxSnapshotID()) {
      // We have reached the maximum allowable snapshot ID and since we don't
      // handle rollover we will fail all subsequent snapshot creation
      // requests.
      //
      throw new SnapshotException(
          "Failed to create the snapshot. The FileSystem has run out of " +
          "snapshot IDs and ID rollover is not supported.");
    }

    srcRoot.addSnapshot(snapshotCounter, snapshotName);
      
    //create success, update id
    snapshotCounter++;
    numSnapshots.getAndIncrement();
    return Snapshot.getSnapshotPath(path, snapshotName);
  }
  
  /**
   * Delete a snapshot for a snapshottable directory
   * @param path Path to the directory where the snapshot was taken
   * @param snapshotName Name of the snapshot to be deleted
   * @param collectedBlocks Used to collect information to update blocksMap 
   * @throws IOException
   */
  public void deleteSnapshot(final String path, final String snapshotName,
      BlocksMapUpdateInfo collectedBlocks, final List<INode> removedINodes)
      throws IOException {
    // parse the path, and check if the path is a snapshot path
    // the INodeDirectorySnapshottable#valueOf method will throw Exception 
    // if the path is not for a snapshottable directory
    INodeDirectory srcRoot = getSnapshottableRoot(path);
    srcRoot.removeSnapshot(snapshotName, collectedBlocks, removedINodes);
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
    final INodeDirectory srcRoot = getSnapshottableRoot(path);
    // Note that renameSnapshot and createSnapshot are synchronized externally
    // through FSNamesystem's write lock
    srcRoot.renameSnapshot(path, oldSnapshotName, newSnapshotName);
  }
  
  public int getNumSnapshottableDirs() {
    return snapshottables.size();
  }

  public int getNumSnapshots() {
    return numSnapshots.get();
  }
  
  void setNumSnapshots(int num) {
    numSnapshots.set(num);
  }

  int getSnapshotCounter() {
    return snapshotCounter;
  }

  void setSnapshotCounter(int counter) {
    snapshotCounter = counter;
  }

  INodeDirectory[] getSnapshottableDirs() {
    return snapshottables.values().toArray(
        new INodeDirectory[snapshottables.size()]);
  }

  /**
   * Write {@link #snapshotCounter}, {@link #numSnapshots},
   * and all snapshots to the DataOutput.
   */
  public void write(DataOutput out) throws IOException {
    out.writeInt(snapshotCounter);
    out.writeInt(numSnapshots.get());

    // write all snapshots.
    for(INodeDirectory snapshottableDir : snapshottables.values()) {
      for (Snapshot s : snapshottableDir.getDirectorySnapshottableFeature()
          .getSnapshotList()) {
        s.write(out);
      }
    }
  }
  
  /**
   * Read values of {@link #snapshotCounter}, {@link #numSnapshots}, and
   * all snapshots from the DataInput
   */
  public Map<Integer, Snapshot> read(DataInput in, FSImageFormat.Loader loader
      ) throws IOException {
    snapshotCounter = in.readInt();
    numSnapshots.set(in.readInt());
    
    // read snapshots
    final Map<Integer, Snapshot> snapshotMap = new HashMap<Integer, Snapshot>();
    for(int i = 0; i < numSnapshots.get(); i++) {
      final Snapshot s = Snapshot.read(in, loader);
      snapshotMap.put(s.getId(), s);
    }
    return snapshotMap;
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
    for (INodeDirectory dir : snapshottables.values()) {
      if (userName == null || userName.equals(dir.getUserName())) {
        SnapshottableDirectoryStatus status = new SnapshottableDirectoryStatus(
            dir.getModificationTime(), dir.getAccessTime(),
            dir.getFsPermission(), dir.getUserName(), dir.getGroupName(),
            dir.getLocalNameBytes(), dir.getId(), 
            dir.getChildrenNum(Snapshot.CURRENT_STATE_ID),
            dir.getDirectorySnapshottableFeature().getNumSnapshots(),
            dir.getDirectorySnapshottableFeature().getSnapshotQuota(),
            dir.getParent() == null ? DFSUtil.EMPTY_BYTES :
                DFSUtil.string2Bytes(dir.getParent().getFullPathName()));
        statusList.add(status);
      }
    }
    Collections.sort(statusList, SnapshottableDirectoryStatus.COMPARATOR);
    return statusList.toArray(
        new SnapshottableDirectoryStatus[statusList.size()]);
  }
  
  /**
   * Compute the difference between two snapshots of a directory, or between a
   * snapshot of the directory and its current tree.
   */
  public SnapshotDiffReport diff(final String path, final String from,
      final String to) throws IOException {
    // Find the source root directory path where the snapshots were taken.
    // All the check for path has been included in the valueOf method.
    final INodeDirectory snapshotRoot = getSnapshottableRoot(path);

    if ((from == null || from.isEmpty())
        && (to == null || to.isEmpty())) {
      // both fromSnapshot and toSnapshot indicate the current tree
      return new SnapshotDiffReport(path, from, to,
          Collections.<DiffReportEntry> emptyList());
    }
    final SnapshotDiffInfo diffs = snapshotRoot
        .getDirectorySnapshottableFeature().computeDiff(snapshotRoot, from, to);
    return diffs != null ? diffs.generateReport() : new SnapshotDiffReport(
        path, from, to, Collections.<DiffReportEntry> emptyList());
  }
  
  public void clearSnapshottableDirs() {
    snapshottables.clear();
  }

  /**
   * Returns the maximum allowable snapshot ID based on the bit width of the
   * snapshot ID.
   *
   * @return maximum allowable snapshot ID.
   */
   public int getMaxSnapshotID() {
    return ((1 << SNAPSHOT_ID_BIT_WIDTH) - 1);
  }

  private ObjectName mxBeanName;

  public void registerMXBean() {
    mxBeanName = MBeans.register("NameNode", "SnapshotInfo", this);
  }

  public void shutdown() {
    MBeans.unregister(mxBeanName);
    mxBeanName = null;
  }

  @Override // SnapshotStatsMXBean
  public SnapshottableDirectoryStatus.Bean[]
    getSnapshottableDirectories() {
    List<SnapshottableDirectoryStatus.Bean> beans =
        new ArrayList<SnapshottableDirectoryStatus.Bean>();
    for (INodeDirectory d : getSnapshottableDirs()) {
      beans.add(toBean(d));
    }
    return beans.toArray(new SnapshottableDirectoryStatus.Bean[beans.size()]);
  }

  @Override // SnapshotStatsMXBean
  public SnapshotInfo.Bean[] getSnapshots() {
    List<SnapshotInfo.Bean> beans = new ArrayList<SnapshotInfo.Bean>();
    for (INodeDirectory d : getSnapshottableDirs()) {
      for (Snapshot s : d.getDirectorySnapshottableFeature().getSnapshotList()) {
        beans.add(toBean(s));
      }
    }
    return beans.toArray(new SnapshotInfo.Bean[beans.size()]);
  }

  public static SnapshottableDirectoryStatus.Bean toBean(INodeDirectory d) {
    return new SnapshottableDirectoryStatus.Bean(
        d.getFullPathName(),
        d.getDirectorySnapshottableFeature().getNumSnapshots(),
        d.getDirectorySnapshottableFeature().getSnapshotQuota(),
        d.getModificationTime(),
        Short.valueOf(Integer.toOctalString(
            d.getFsPermissionShort())),
        d.getUserName(),
        d.getGroupName());
  }

  public static SnapshotInfo.Bean toBean(Snapshot s) {
    return new SnapshotInfo.Bean(
        s.getRoot().getLocalName(), s.getRoot().getFullPathName(),
        s.getRoot().getModificationTime());
  }
}
