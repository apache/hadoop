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
public interface FileWithSnapshot {
  /** @return the {@link INodeFile} view of this object. */
  public INodeFile asINodeFile();
  
  /** @return the next element. */
  public FileWithSnapshot getNext();

  /** Set the next element. */
  public void setNext(FileWithSnapshot next);
  
  /** Insert inode to the circular linked list, after the current node. */
  public void insertAfter(FileWithSnapshot inode);
  
  /** Insert inode to the circular linked list, before the current node. */
  public void insertBefore(FileWithSnapshot inode);
  
  /** Remove self from the circular list */
  public void removeSelf();
  
  /** Utility methods for the classes which implement the interface. */
  static class Util {

    /** @return The previous node in the circular linked list */
    static FileWithSnapshot getPrevious(FileWithSnapshot file) {
      FileWithSnapshot previous = file.getNext();
      while (previous.getNext() != file) {
        previous = previous.getNext();
      }
      return previous;
    }
    
    /** Replace the old file with the new file in the circular linked list. */
    static void replace(FileWithSnapshot oldFile, FileWithSnapshot newFile) {
      //set next element
      FileWithSnapshot i = oldFile.getNext();
      newFile.setNext(i);
      oldFile.setNext(null);
      //find previous element and update it
      for(; i.getNext() != oldFile; i = i.getNext());
      i.setNext(newFile);
    }

    /**
     * @return the max file replication of the elements
     *         in the circular linked list.
     */
    static short getBlockReplication(final FileWithSnapshot file) {
      short max = file.asINodeFile().getFileReplication();
      // i may be null since next will be set to null when the INode is deleted
      for(FileWithSnapshot i = file.getNext();
          i != file && i != null;
          i = i.getNext()) {
        final short replication = i.asINodeFile().getFileReplication();
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
    static int collectSubtreeBlocksAndClear(final FileWithSnapshot file,
        final BlocksMapUpdateInfo info) {
      final FileWithSnapshot next = file.getNext();
      Preconditions.checkState(next != file, "this is the only remaining inode.");

      // There are other inode(s) using the blocks.
      // Compute max file size excluding this and find the last inode.
      long max = next.asINodeFile().computeFileSize(true);
      short maxReplication = next.asINodeFile().getFileReplication();
      FileWithSnapshot last = next;
      for(FileWithSnapshot i = next.getNext(); i != file; i = i.getNext()) {
        final long size = i.asINodeFile().computeFileSize(true);
        if (size > max) {
          max = size;
        }
        final short rep = i.asINodeFile().getFileReplication();
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
      file.asINodeFile().setFileReplication(maxReplication, null);
      file.setNext(null);
      // clear parent
      file.asINodeFile().setParent(null);
      return 1;
    }

    static void collectBlocksBeyondMaxAndClear(final FileWithSnapshot file,
            final long max, final BlocksMapUpdateInfo info) {
      final BlockInfo[] oldBlocks = file.asINodeFile().getBlocks();
      if (oldBlocks != null) {
        //find the minimum n such that the size of the first n blocks > max
        int n = 0;
        for(long size = 0; n < oldBlocks.length && max > size; n++) {
          size += oldBlocks[n].getNumBytes();
        }

        // Replace the INode for all the remaining blocks in blocksMap
        final FileWithSnapshot next = file.getNext();
        final BlocksMapINodeUpdateEntry entry = new BlocksMapINodeUpdateEntry(
            file.asINodeFile(), next.asINodeFile());
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
          for(FileWithSnapshot i = next; i != file; i = i.getNext()) {
            i.asINodeFile().setBlocks(newBlocks);
          }

          // collect the blocks beyond max.  
          if (info != null) {
            for(; n < oldBlocks.length; n++) {
              info.addDeleteBlock(oldBlocks[n]);
            }
          }
        }
        file.asINodeFile().setBlocks(null);
      }
    }
  }
}
