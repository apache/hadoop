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
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapINodeUpdateEntry;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;

import com.google.common.base.Preconditions;

/**
 * {@link INodeFile} with a link to the next element.
 * The link of all the snapshot files and the original file form a circular
 * linked list so that all elements are accessible by any of the elements.
 */
@InterfaceAudience.Private
public interface FileWithLink {
  /** @return the next element. */
  public <N extends INodeFile & FileWithLink> N getNext();

  /** Set the next element. */
  public <N extends INodeFile & FileWithLink> void setNext(N next);
  
  /** Insert inode to the circular linked list. */
  public <N extends INodeFile & FileWithLink> void insert(N inode);
  
  /** Utility methods for the classes which implement the interface. */
  static class Util {
    /**
     * @return the max file replication of the elements
     *         in the circular linked list.
     */
    static <F extends INodeFile & FileWithLink,
            N extends INodeFile & FileWithLink> short getBlockReplication(
        final F file) {
      short max = file.getFileReplication();
      // i may be null since next will be set to null when the INode is deleted
      for(N i = file.getNext(); i != file && i != null; i = i.getNext()) {
        final short replication = i.getFileReplication();
        if (replication > max) {
          max = replication;
        }
      }
      return max;
    }

    /**
     * Remove the current inode from the circular linked list.
     * If some blocks at the end of the block list no longer belongs to
     * any other inode, collect them and update the block list.
     */
    static <F extends INodeFile & FileWithLink,
            N extends INodeFile & FileWithLink>
        int collectSubtreeBlocksAndClear(F file, BlocksMapUpdateInfo info) {
      final N next = file.getNext();
      Preconditions.checkState(next != file, "this is the only remaining inode.");

      // There are other inode(s) using the blocks.
      // Compute max file size excluding this and find the last inode.
      long max = next.computeFileSize(true);
      short maxReplication = next.getFileReplication();
      FileWithLink last = next;
      for(N i = next.getNext(); i != file; i = i.getNext()) {
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

      collectBlocksBeyondMaxAndClear(file, max, info);
      
      // remove this from the circular linked list.
      last.setNext(next);
      // Set the replication of the current INode to the max of all the other
      // linked INodes, so that in case the current INode is retrieved from the
      // blocksMap before it is removed or updated, the correct replication
      // number can be retrieved.
      file.setFileReplication(maxReplication, null);
      file.setNext(null);
      // clear parent
      file.setParent(null);
      return 1;
    }

    static <F extends INodeFile & FileWithLink,
            N extends INodeFile & FileWithLink>
        void collectBlocksBeyondMaxAndClear(final F file,
            final long max, final BlocksMapUpdateInfo info) {
      final BlockInfo[] oldBlocks = file.getBlocks();
      if (oldBlocks != null) {
        //find the minimum n such that the size of the first n blocks > max
        int n = 0;
        for(long size = 0; n < oldBlocks.length && max > size; n++) {
          size += oldBlocks[n].getNumBytes();
        }

        // Replace the INode for all the remaining blocks in blocksMap
        final N next = file.getNext();
        final BlocksMapINodeUpdateEntry entry = new BlocksMapINodeUpdateEntry(
            file, next);
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
          for(N i = next; i != file; i = i.getNext()) {
            i.setBlocks(newBlocks);
          }

          // collect the blocks beyond max.  
          if (info != null) {
            for(; n < oldBlocks.length; n++) {
              info.addDeleteBlock(oldBlocks[n]);
            }
          }
        }
        file.setBlocks(null);
      }
    }
  }
}
