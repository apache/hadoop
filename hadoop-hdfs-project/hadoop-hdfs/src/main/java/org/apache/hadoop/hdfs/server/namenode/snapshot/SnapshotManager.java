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

import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;

/** Manage snapshottable directories and their snapshots. */
public class SnapshotManager {
  private final Namesystem namesystem;

  /** All snapshottable directories in the namesystem. */
  private final List<INodeDirectorySnapshottable> snapshottables
      = new ArrayList<INodeDirectorySnapshottable>();

  public SnapshotManager(final Namesystem namesystem) {
    this.namesystem = namesystem;
  }

  /**
   * Set the given directory as a snapshottable directory.
   * If the path is already a snapshottable directory, this is a no-op.
   * Otherwise, the {@link INodeDirectory} of the path is replaced by an 
   * {@link INodeDirectorySnapshottable}.
   */
  public void setSnapshottable(final String path,
      final FSDirectory fsdir) throws IOException {
    namesystem.writeLock();
    try {
      final INodeDirectory d = INodeDirectory.valueOf(fsdir.getINode(path), path);
      if (d.isSnapshottable()) {
        //The directory is already a snapshottable directory. 
        return;
      }

      final INodeDirectorySnapshottable s
          = INodeDirectorySnapshottable.newInstance(d);
      fsdir.replaceINodeDirectory(path, d, s);
      snapshottables.add(s);
    } finally {
      namesystem.writeUnlock();
    }
  }

  /** Create a snapshot of given path. */
  public void createSnapshot(final String snapshotName, final String path,
      final FSDirectory fsdir) throws IOException {
    final INodeDirectorySnapshottable d = INodeDirectorySnapshottable.valueOf(
        fsdir.getINode(path), path);

    //TODO: check ns quota
    
    final INodeDirectorySnapshotRoot root = d.addSnapshotRoot(snapshotName);
    
    //TODO: create the remaining subtree
  }
}