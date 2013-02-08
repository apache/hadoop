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
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapINodeUpdateEntry;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 * {@link INodeFile} with a link to the next element.
 * The link of all the snapshot files and the original file form a circular
 * linked list so that all elements are accessible by any of the elements.
 */
@InterfaceAudience.Private
public interface FileWithSnapshot {
  /**
   * The difference of an {@link INodeFile} between two snapshots.
   */
  static class FileDiff extends AbstractINodeDiff<INodeFile, FileDiff> {
    /** The file size at snapshot creation time. */
    final long fileSize;

    FileDiff(Snapshot snapshot, INodeFile file) {
      super(snapshot, null, null);
      fileSize = file.computeFileSize(true, null);
    }

    @Override
    INodeFile createSnapshotCopyOfCurrentINode(INodeFile currentINode) {
      final INodeFile copy = new INodeFile(currentINode);
      copy.setBlocks(null);
      return copy;
    }

    @Override
    void combinePosteriorAndCollectBlocks(INodeFile currentINode,
        FileDiff posterior, BlocksMapUpdateInfo collectedBlocks) {
      Util.collectBlocksAndClear((FileWithSnapshot)currentINode, collectedBlocks);
    }
    
    @Override
    public String toString() {
      return super.toString() + " fileSize=" + fileSize + ", rep="
          + (snapshotINode == null? "?": snapshotINode.getFileReplication());
    }
  }

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

  /** Is the current file deleted? */
  public boolean isCurrentFileDeleted();

  /** Are the current file and all snapshot copies deleted? */
  public boolean isEverythingDeleted();

  /** @return the max file replication in the inode and its snapshot copies. */
  public short getMaxFileReplication();
  
  /** @return the max file size in the inode and its snapshot copies. */
  public long computeMaxFileSize();

  /** Utility methods for the classes which implement the interface. */
  public static class Util {
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
      final FileWithSnapshot oldNext = oldFile.getNext();
      if (oldNext == null) {
        newFile.setNext(null);
      } else {
        if (oldNext != oldFile) {
          newFile.setNext(oldNext);
          getPrevious(oldFile).setNext(newFile);
        }
        oldFile.setNext(null);
      }
    }

    /** @return the max file replication of the file in the diff list. */
    static <N extends INodeFile, D extends AbstractINodeDiff<N, D>>
        short getMaxFileReplication(short max,
              final AbstractINodeDiffList<N, D> diffs) {
      for(AbstractINodeDiff<N, D> d : diffs) {
        if (d.snapshotINode != null) {
          final short replication = d.snapshotINode.getFileReplication();
          if (replication > max) {
            max = replication;
          }
        }
      }
      return max;
    }

    /**
     * @return the max file replication of the elements
     *         in the circular linked list.
     */
    static short getBlockReplication(final FileWithSnapshot file) {
      short max = file.getMaxFileReplication();
      // i may be null since next will be set to null when the INode is deleted
      for(FileWithSnapshot i = file.getNext();
          i != file && i != null;
          i = i.getNext()) {
        final short replication = i.getMaxFileReplication();
        if (replication > max) {
          max = replication;
        }
      }
      return max;
    }

    /**
     * If some blocks at the end of the block list no longer belongs to
     * any inode, collect them and update the block list.
     */
    static void collectBlocksAndClear(final FileWithSnapshot file,
        final BlocksMapUpdateInfo info) {
      final FileWithSnapshot next = file.getNext();

      // find max file size, max replication and the last inode.
      long maxFileSize = file.computeMaxFileSize();
      short maxReplication = file.getMaxFileReplication();
      FileWithSnapshot last = null;
      if (next != null && next != file) {
        for(FileWithSnapshot i = next; i != file; i = i.getNext()) {
          final long size = i.computeMaxFileSize();
          if (size > maxFileSize) {
            maxFileSize = size;
          }
          final short rep = i.getMaxFileReplication();
          if (rep > maxReplication) {
            maxReplication = rep;
          }
          last = i;
        }
      }

      collectBlocksBeyondMax(file, maxFileSize, info);

      if (file.isEverythingDeleted()) {
        // Set the replication of the current INode to the max of all the other
        // linked INodes, so that in case the current INode is retrieved from the
        // blocksMap before it is removed or updated, the correct replication
        // number can be retrieved.
        if (maxReplication > 0) {
          file.asINodeFile().setFileReplication(maxReplication, null);
        }

        // remove the file from the circular linked list.
        if (last != null) {
          last.setNext(next);
        }
        file.setNext(null);

        file.asINodeFile().setBlocks(null);
      }
    }

    private static void collectBlocksBeyondMax(final FileWithSnapshot file,
        final long max, final BlocksMapUpdateInfo collectedBlocks) {
      final BlockInfo[] oldBlocks = file.asINodeFile().getBlocks();
      if (oldBlocks != null) {
        //find the minimum n such that the size of the first n blocks > max
        int n = 0;
        for(long size = 0; n < oldBlocks.length && max > size; n++) {
          size += oldBlocks[n].getNumBytes();
        }

        // collect update blocks
        final FileWithSnapshot next = file.getNext();
        if (next != null && next != file && file.isEverythingDeleted() && collectedBlocks != null) {
          final BlocksMapINodeUpdateEntry entry = new BlocksMapINodeUpdateEntry(
              file.asINodeFile(), next.asINodeFile());
          for (int i = 0; i < n; i++) {
            collectedBlocks.addUpdateBlock(oldBlocks[i], entry);
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
          
          // set new blocks
          file.asINodeFile().setBlocks(newBlocks);
          for(FileWithSnapshot i = next; i != null && i != file; i = i.getNext()) {
            i.asINodeFile().setBlocks(newBlocks);
          }

          // collect the blocks beyond max.  
          if (collectedBlocks != null) {
            for(; n < oldBlocks.length; n++) {
              collectedBlocks.addDeleteBlock(oldBlocks[n]);
            }
          }
        }
      }
    }
    
    static String circularListString(final FileWithSnapshot file) {
      final StringBuilder b = new StringBuilder("* -> ")
          .append(file.asINodeFile().getObjectString());
      FileWithSnapshot n = file.getNext();
      for(; n != null && n != file; n = n.getNext()) {
        b.append(" -> ").append(n.asINodeFile().getObjectString());
      }
      return b.append(n == null? " -> null": " -> *").toString();
    }
  }
}
