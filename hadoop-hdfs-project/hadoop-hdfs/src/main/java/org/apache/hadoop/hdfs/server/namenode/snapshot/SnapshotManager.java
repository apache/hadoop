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
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.INodeSymlink;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

/** Manage snapshottable directories and their snapshots. */
public class SnapshotManager implements SnapshotStats {
  private final FSNamesystem namesystem;
  private final FSDirectory fsdir;

  private final AtomicInteger numSnapshottableDirs = new AtomicInteger();
  private final AtomicInteger numSnapshots = new AtomicInteger();

  private int snapshotID = 0;
  
  /** All snapshottable directories in the namesystem. */
  private final List<INodeDirectorySnapshottable> snapshottables
      = new ArrayList<INodeDirectorySnapshottable>();

  public SnapshotManager(final FSNamesystem namesystem,
      final FSDirectory fsdir) {
    this.namesystem = namesystem;
    this.fsdir = fsdir;
  }

  /**
   * Set the given directory as a snapshottable directory.
   * If the path is already a snapshottable directory, update the quota.
   */
  public void setSnapshottable(final String path, final int snapshotQuota
      ) throws IOException {
    final INodeDirectory d = INodeDirectory.valueOf(fsdir.getINode(path), path);
    if (d.isSnapshottable()) {
      //The directory is already a snapshottable directory.
      ((INodeDirectorySnapshottable)d).setSnapshotQuota(snapshotQuota);
      return;
    }

    final INodeDirectorySnapshottable s
        = INodeDirectorySnapshottable.newInstance(d, snapshotQuota);
    fsdir.replaceINodeDirectory(path, d, s);
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
    final INodeDirectorySnapshottable s = INodeDirectorySnapshottable.valueOf(
        fsdir.getINode(path), path);
    if (s.getNumSnapshots() > 0) {
      throw new SnapshotException("The directory " + path + " has snapshot(s). "
          + "Please redo the operation after removing all the snapshots.");
    }

    final INodeDirectory d = new INodeDirectory(s);
    fsdir.replaceINodeDirectory(path, s, d);
    snapshottables.remove(s);

    numSnapshottableDirs.getAndDecrement();
  }

  /**
   * Create a snapshot of the given path.
   * 
   * @param snapshotName The name of the snapshot.
   * @param path The directory path where the snapshot will be taken.
   */
  public void createSnapshot(final String snapshotName, final String path
      ) throws IOException {
    // Find the source root directory path where the snapshot is taken.
    final INodeDirectorySnapshottable srcRoot
        = INodeDirectorySnapshottable.valueOf(fsdir.getINode(path), path);

    synchronized(this) {
      final Snapshot s = srcRoot.addSnapshot(snapshotID, snapshotName);
      new SnapshotCreation().processRecursively(srcRoot, s.getRoot());
      
      //create success, update id
      snapshotID++;
    }
    numSnapshots.getAndIncrement();
  }
  
  /**
   * Create a snapshot of subtrees by recursively coping the directory
   * structure from the source directory to the snapshot destination directory.
   * This creation algorithm requires O(N) running time and O(N) memory,
   * where N = # files + # directories + # symlinks. 
   */
  class SnapshotCreation {
    /** Process snapshot creation recursively. */
    private void processRecursively(final INodeDirectory srcDir,
        final INodeDirectory dstDir) throws IOException {
      final ReadOnlyList<INode> children = srcDir.getChildrenList(null);
      if (!children.isEmpty()) {
        final List<INode> inodes = new ArrayList<INode>(children.size());
        for(final INode c : new ArrayList<INode>(ReadOnlyList.Util.asList(children))) {
          final INode i;
          if (c == null) {
            i = null;
          } else if (c instanceof INodeDirectory) {
            //also handle INodeDirectoryWithQuota
            i = processINodeDirectory((INodeDirectory)c);
          } else if (c instanceof INodeFileUnderConstruction) {
            //TODO: support INodeFileUnderConstruction
            throw new IOException("Not yet supported.");
          } else if (c instanceof INodeFile) {
            i = processINodeFile(srcDir, (INodeFile)c);
          } else if (c instanceof INodeSymlink) {
            i = new INodeSymlink((INodeSymlink)c);
          } else {
            throw new AssertionError("Unknow INode type: " + c.getClass()
                + ", inode = " + c);
          }
          i.setParent(dstDir);
          inodes.add(i);
        }
        dstDir.setChildren(inodes);
      }
    }
    
    /**
     * Create destination INodeDirectory and make the recursive call. 
     * @return destination INodeDirectory.
     */
    private INodeDirectory processINodeDirectory(final INodeDirectory srcChild
        ) throws IOException {
      final INodeDirectory dstChild = new INodeDirectory(srcChild);
      dstChild.setChildren(null);
      processRecursively(srcChild, dstChild);
      return dstChild;
    }

    /**
     * Create destination INodeFileSnapshot and update source INode type.
     * @return destination INodeFileSnapshot.
     */
    private INodeFileSnapshot processINodeFile(final INodeDirectory parent,
        final INodeFile file) {
      final INodeFileSnapshot snapshot = new INodeFileSnapshot(
          file, file.computeFileSize(true)); 

      final INodeFileWithLink srcWithLink;
      //check source INode type
      if (file instanceof INodeFileWithLink) {
        srcWithLink = (INodeFileWithLink)file;
      } else {
        //source is an INodeFile, replace the source.
        srcWithLink = new INodeFileWithLink(file);
        file.removeNode();
        parent.addChild(srcWithLink, false);

        //update block map
        namesystem.getBlockManager().addBlockCollection(srcWithLink);
      }
      
      //insert the snapshot to src's linked list.
      srcWithLink.insert(snapshot);
      return snapshot;
    }
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