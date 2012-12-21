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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory.INodesInPath;

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

  private int snapshotID = 0;
  
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
    final INodesInPath iip = fsdir.getINodesInPath(path);
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
    final INodesInPath iip = fsdir.getINodesInPath(path);
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
   * @param snapshotName
   *          The name of the snapshot.
   * @param path
   *          The directory path where the snapshot will be taken.
   * @throws IOException
   *           Throw IOException when 1) the given path does not lead to an
   *           existing snapshottable directory, and/or 2) there exists a
   *           snapshot with the given name for the directory, and/or 3)
   *           snapshot number exceeds quota
   */
  public void createSnapshot(final String snapshotName, final String path
      ) throws IOException {
    // Find the source root directory path where the snapshot is taken.
    final INodesInPath i = fsdir.getMutableINodesInPath(path);
    final INodeDirectorySnapshottable srcRoot
        = INodeDirectorySnapshottable.valueOf(i.getLastINode(), path);
    srcRoot.addSnapshot(snapshotID, snapshotName);
      
    //create success, update id
    snapshotID++;
    numSnapshots.getAndIncrement();
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
  
}