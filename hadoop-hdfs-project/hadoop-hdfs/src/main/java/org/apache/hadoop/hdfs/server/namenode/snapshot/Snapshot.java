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

import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSImageSerialization;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

/** Snapshot of a sub-tree in the namesystem. */
@InterfaceAudience.Private
public class Snapshot implements Comparable<byte[]> {
  /**
   * Compare snapshot IDs. Null indicates the current status thus is greater
   * than non-null snapshots.
   */
  public static final Comparator<Snapshot> ID_COMPARATOR
      = new Comparator<Snapshot>() {
    @Override
    public int compare(Snapshot left, Snapshot right) {
      // null means the current state, thus should be the largest
      if (left == null) {
        return right == null? 0: 1;
      } else {
        return right == null? -1: left.id - right.id; 
      }
    }
  };

  /**
   * Find the latest snapshot that 1) covers the given inode (which means the
   * snapshot was either taken on the inode or taken on an ancestor of the
   * inode), and 2) was taken before the given snapshot (if the given snapshot 
   * is not null).
   * 
   * @param inode the given inode that the returned snapshot needs to cover
   * @param anchor the returned snapshot should be taken before this snapshot.
   * @return the latest snapshot covers the given inode and was taken before the
   *         the given snapshot (if it is not null).
   */
  public static Snapshot findLatestSnapshot(INode inode, Snapshot anchor) {
    Snapshot latest = null;
    for(; inode != null; inode = inode.getParent()) {
      if (inode instanceof INodeDirectoryWithSnapshot) {
        final Snapshot s = ((INodeDirectoryWithSnapshot) inode).getDiffs()
            .getPrior(anchor);
        if (latest == null
            || (s != null && ID_COMPARATOR.compare(latest, s) < 0)) {
          latest = s;
        }
      }
    }
    return latest;
  }
  
  /** 
   * Get the name of the given snapshot. 
   * @param s The given snapshot.
   * @return The name of the snapshot, or an empty string if {@code s} is null
   */
  public static String getSnapshotName(Snapshot s) {
    return s != null ? s.getRoot().getLocalName() : "";
  }

  /** The root directory of the snapshot. */
  public class Root extends INodeDirectory {
    Root(INodeDirectory other) {
      super(other, false);
    }

    @Override
    public ReadOnlyList<INode> getChildrenList(Snapshot snapshot) {
      return getParent().getChildrenList(snapshot);
    }

    @Override
    public INode getChild(byte[] name, Snapshot snapshot) {
      return getParent().getChild(name, snapshot);
    }
    
    @Override
    public String getFullPathName() {
      return getParent().getFullPathName() + Path.SEPARATOR
          + HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR
          + this.getLocalName();
    }
  }

  /** Snapshot ID. */
  private final int id;
  /** The root directory of the snapshot. */
  private final Root root;

  Snapshot(int id, String name, INodeDirectorySnapshottable dir) {
    this(id, dir, dir);
    this.root.setLocalName(DFSUtil.string2Bytes(name));
  }

  Snapshot(int id, INodeDirectory dir,
      INodeDirectorySnapshottable parent) {
    this.id = id;
    this.root = new Root(dir);

    this.root.setParent(parent);
  }
  
  /** @return the root directory of the snapshot. */
  public Root getRoot() {
    return root;
  }

  @Override
  public int compareTo(byte[] bytes) {
    return root.compareTo(bytes);
  }
  
  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    } else if (that == null || !(that instanceof Snapshot)) {
      return false;
    }
    return this.id == ((Snapshot)that).id;
  }
  
  @Override
  public int hashCode() {
    return id;
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName() + "." + root.getLocalName();
  }
  
  /** Serialize the fields to out */
  void write(DataOutput out) throws IOException {
    out.writeInt(id);
    // write root
    FSImageSerialization.writeINodeDirectory(root, out);
  }
}
