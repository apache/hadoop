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
  /** Compare snapshot IDs with null <= s for any snapshot s. */
  public static final Comparator<Snapshot> ID_COMPARATOR
      = new Comparator<Snapshot>() {
    @Override
    public int compare(Snapshot left, Snapshot right) {
      if (left == null) {
        return right == null? 0: -1;
      } else {
        return right == null? 1: left.id - right.id; 
      }
    }
  };

  /** @return the latest snapshot taken on the given inode. */
  public static Snapshot findLatestSnapshot(INode inode) {
    Snapshot latest = null;
    for(; inode != null; inode = inode.getParent()) {
      if (inode instanceof INodeDirectorySnapshottable) {
        final Snapshot s = ((INodeDirectorySnapshottable)inode).getLastSnapshot();
        if (ID_COMPARATOR.compare(latest, s) < 0) {
          latest = s;
        }
      }
    }
    return latest;
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
