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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * INodeFile with a link to the next element.
 * This class is used to represent the original file that is snapshotted.
 * The snapshot files are represented by {@link INodeFileSnapshot}.
 * The link of all the snapshot files and the original file form a circular
 * linked list so that all elements are accessible by any of the elements.
 */
@InterfaceAudience.Private
public class INodeFileWithLink extends INodeFile {
  private INodeFileWithLink next;

  public INodeFileWithLink(INodeFile f) {
    super(f);
    next = this;
  }

  @Override
  public Pair<INodeFileWithLink, INodeFileSnapshot> createSnapshotCopy() {
    return new Pair<INodeFileWithLink, INodeFileSnapshot>(this,
        new INodeFileSnapshot(this));
  }

  void setNext(INodeFileWithLink next) {
    this.next = next;
  }

  INodeFileWithLink getNext() {
    return next;
  }
  
  /** Insert inode to the circular linked list. */
  void insert(INodeFileWithLink inode) {
    inode.setNext(this.getNext());
    this.setNext(inode);
  }

  /**
   * @return the max file replication of the elements
   *         in the circular linked list.
   */
  @Override
  public short getBlockReplication() {
    short max = getFileReplication();
    // i may be null since next will be set to null when the INode is deleted
    for(INodeFileWithLink i = next; i != this && i != null; i = i.getNext()) {
      final short replication = i.getFileReplication();
      if (replication > max) {
        max = replication;
      }
    }
    return max;
  }

  /**
   * {@inheritDoc}
   * 
   * Remove the current inode from the circular linked list.
   * If some blocks at the end of the block list no longer belongs to
   * any other inode, collect them and update the block list.
   */
  @Override
  protected int collectSubtreeBlocksAndClear(BlocksMapUpdateInfo info) {
    if (next == this) {
      // this is the only remaining inode.
      super.collectSubtreeBlocksAndClear(info);
    } else {
      // There are other inode(s) using the blocks.
      // Compute max file size excluding this and find the last inode. 
      long max = next.computeFileSize(true);
      short maxReplication = next.getFileReplication();
      INodeFileWithLink last = next;
      for(INodeFileWithLink i = next.getNext(); i != this; i = i.getNext()) {
        final long size = i.computeFileSize(true);
        if (size > max) {
          max = size;
        }
        final short rep = i.getFileReplication();
        if (rep > maxReplication) {
          maxReplication = rep;
        }
        last = i;
      }

      collectBlocksBeyondMaxAndClear(max, info);
      
      // remove this from the circular linked list.
      last.next = this.next;
      // Set the replication of the current INode to the max of all the other
      // linked INodes, so that in case the current INode is retrieved from the
      // blocksMap before it is removed or updated, the correct replication
      // number can be retrieved.
      this.setFileReplication(maxReplication, null);
      this.next = null;
      // clear parent
      setParent(null);
    }
    return 1;
  }

  private void collectBlocksBeyondMaxAndClear(final long max,
      final BlocksMapUpdateInfo info) {
    final BlockInfo[] oldBlocks = getBlocks();
    if (oldBlocks != null) {
      //find the minimum n such that the size of the first n blocks > max
      int n = 0;
      for(long size = 0; n < oldBlocks.length && max > size; n++) {
        size += oldBlocks[n].getNumBytes();
      }

      // Replace the INode for all the remaining blocks in blocksMap
      BlocksMapINodeUpdateEntry entry = new BlocksMapINodeUpdateEntry(this,
          next);
      if (info != null) {
        for (int i = 0; i < n; i++) {
          info.addUpdateBlock(oldBlocks[i], entry);
        }
      }
      
      // starting from block n, the data is beyond max.
      if (n < oldBlocks.length) {
        // resize the array.  
        final BlockInfo[] newBlocks;
        if (n == 0) {
          newBlocks = null;
        } else {
          newBlocks = new BlockInfo[n];
          System.arraycopy(oldBlocks, 0, newBlocks, 0, n);
        }
        for(INodeFileWithLink i = next; i != this; i = i.getNext()) {
          i.setBlocks(newBlocks);
        }

        // collect the blocks beyond max.  
        if (info != null) {
          for(; n < oldBlocks.length; n++) {
            info.addDeleteBlock(oldBlocks[n]);
          }
        }
      }
      setBlocks(null);
    }
  }
}
