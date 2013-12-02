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

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeMap;
import org.apache.hadoop.hdfs.server.namenode.Quota;

/**
 * Represent an {@link INodeFile} that is snapshotted.
 */
@InterfaceAudience.Private
public class INodeFileWithSnapshot extends INodeFile {
  private final FileDiffList diffs;
  private boolean isCurrentFileDeleted = false;

  public INodeFileWithSnapshot(INodeFile f) {
    this(f, f instanceof INodeFileWithSnapshot ? 
        ((INodeFileWithSnapshot) f).getDiffs() : null);
  }

  public INodeFileWithSnapshot(INodeFile f, FileDiffList diffs) {
    super(f);
    this.diffs = diffs != null? diffs: new FileDiffList();
  }

  /** Is the current file deleted? */
  public boolean isCurrentFileDeleted() {
    return isCurrentFileDeleted;
  }
  
  /** Delete the file from the current tree */
  public void deleteCurrentFile() {
    isCurrentFileDeleted = true;
  }

  @Override
  public INodeFileAttributes getSnapshotINode(Snapshot snapshot) {
    return diffs.getSnapshotINode(snapshot, this);
  }

  @Override
  public INodeFileWithSnapshot recordModification(final Snapshot latest,
      final INodeMap inodeMap) throws QuotaExceededException {
    if (isInLatestSnapshot(latest) && !shouldRecordInSrcSnapshot(latest)) {
      diffs.saveSelf2Snapshot(latest, this, null);
    }
    return this;
  }

  /** @return the file diff list. */
  public FileDiffList getDiffs() {
    return diffs;
  }

  @Override
  public Quota.Counts cleanSubtree(final Snapshot snapshot, Snapshot prior,
      final BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes, final boolean countDiffChange) 
      throws QuotaExceededException {
    if (snapshot == null) { // delete the current file
      if (!isCurrentFileDeleted()) {
        recordModification(prior, null);
        deleteCurrentFile();
      }
      this.collectBlocksAndClear(collectedBlocks, removedINodes);
      return Quota.Counts.newInstance();
    } else { // delete a snapshot
      prior = getDiffs().updatePrior(snapshot, prior);
      return diffs.deleteSnapshotDiff(snapshot, prior, this, collectedBlocks,
          removedINodes, countDiffChange);
    }
  }

  @Override
  public String toDetailString() {
    return super.toDetailString()
        + (isCurrentFileDeleted()? "(DELETED), ": ", ") + diffs;
  }
  
  /** 
   * @return block replication, which is the max file replication among
   *         the file and the diff list.
   */
  @Override
  public short getBlockReplication() {
    short max = isCurrentFileDeleted() ? 0 : getFileReplication();
    for(FileDiff d : getDiffs()) {
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
   * If some blocks at the end of the block list no longer belongs to
   * any inode, collect them and update the block list.
   */
  void collectBlocksAndClear(final BlocksMapUpdateInfo info,
      final List<INode> removedINodes) {
    // check if everything is deleted.
    if (isCurrentFileDeleted() && getDiffs().asList().isEmpty()) {
      destroyAndCollectBlocks(info, removedINodes);
      return;
    }

    // find max file size.
    final long max;
    if (isCurrentFileDeleted()) {
      final FileDiff last = getDiffs().getLast();
      max = last == null? 0: last.getFileSize();
    } else { 
      max = computeFileSize();
    }

    collectBlocksBeyondMax(max, info);
  }

  private void collectBlocksBeyondMax(final long max,
      final BlocksMapUpdateInfo collectedBlocks) {
    final BlockInfo[] oldBlocks = getBlocks();
    if (oldBlocks != null) {
      //find the minimum n such that the size of the first n blocks > max
      int n = 0;
      for(long size = 0; n < oldBlocks.length && max > size; n++) {
        size += oldBlocks[n].getNumBytes();
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
        setBlocks(newBlocks);

        // collect the blocks beyond max.  
        if (collectedBlocks != null) {
          for(; n < oldBlocks.length; n++) {
            collectedBlocks.addDeleteBlock(oldBlocks[n]);
          }
        }
      }
    }
  }
  
  Quota.Counts updateQuotaAndCollectBlocks(FileDiff removed,
      BlocksMapUpdateInfo collectedBlocks, final List<INode> removedINodes) {
    long oldDiskspace = this.diskspaceConsumed();
    if (removed.snapshotINode != null) {
      short replication = removed.snapshotINode.getFileReplication();
      short currentRepl = getBlockReplication();
      if (currentRepl == 0) {
        oldDiskspace = computeFileSize(true, true) * replication;
      } else if (replication > currentRepl) {  
        oldDiskspace = oldDiskspace / getBlockReplication()
            * replication;
      }
    }
    
    this.collectBlocksAndClear(collectedBlocks, removedINodes);
    
    long dsDelta = oldDiskspace - diskspaceConsumed();
    return Quota.Counts.newInstance(0, dsDelta);
  }
}
