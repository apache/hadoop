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
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileAttributes;
import org.apache.hadoop.hdfs.server.namenode.Quota;

/**
 * Feature for file with snapshot-related information.
 */
@InterfaceAudience.Private
public class FileWithSnapshotFeature implements INode.Feature {
  private final FileDiffList diffs;
  private boolean isCurrentFileDeleted = false;
  
  public FileWithSnapshotFeature(FileDiffList diffs) {
    this.diffs = diffs != null? diffs: new FileDiffList();
  }

  public boolean isCurrentFileDeleted() {
    return isCurrentFileDeleted;
  }
  
  /** 
   * We need to distinguish two scenarios:
   * 1) the file is still in the current file directory, it has been modified 
   *    before while it is included in some snapshot
   * 2) the file is not in the current file directory (deleted), but it is in
   *    some snapshot, thus we still keep this inode
   * For both scenarios the file has snapshot feature. We set 
   * {@link #isCurrentFileDeleted} to true for 2).
   */
  public void deleteCurrentFile() {
    isCurrentFileDeleted = true;
  }

  public FileDiffList getDiffs() {
    return diffs;
  }
  
  /** @return the max replication factor in diffs */
  public short getMaxBlockRepInDiffs() {
    short max = 0;
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

  boolean changedBetweenSnapshots(INodeFile file, Snapshot from, Snapshot to) {
    int[] diffIndexPair = diffs.changedBetweenSnapshots(from, to);
    if (diffIndexPair == null) {
      return false;
    }
    int earlierDiffIndex = diffIndexPair[0];
    int laterDiffIndex = diffIndexPair[1];

    final List<FileDiff> diffList = diffs.asList();
    final long earlierLength = diffList.get(earlierDiffIndex).getFileSize();
    final long laterLength = laterDiffIndex == diffList.size() ? file
        .computeFileSize(true, false) : diffList.get(laterDiffIndex)
        .getFileSize();
    if (earlierLength != laterLength) { // file length has been changed
      return true;
    }

    INodeFileAttributes earlierAttr = null; // check the metadata
    for (int i = earlierDiffIndex; i < laterDiffIndex; i++) {
      FileDiff diff = diffList.get(i);
      if (diff.snapshotINode != null) {
        earlierAttr = diff.snapshotINode;
        break;
      }
    }
    if (earlierAttr == null) { // no meta-change at all, return false
      return false;
    }
    INodeFileAttributes laterAttr = diffs.getSnapshotINode(
        Math.max(Snapshot.getSnapshotId(from), Snapshot.getSnapshotId(to)),
        file);
    return !earlierAttr.metadataEquals(laterAttr);
  }

  public String getDetailedString() {
    return (isCurrentFileDeleted()? "(DELETED), ": ", ") + diffs;
  }
  
  public Quota.Counts cleanFile(final INodeFile file, final int snapshotId,
      int priorSnapshotId, final BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes, final boolean countDiffChange)
      throws QuotaExceededException {
    if (snapshotId == Snapshot.CURRENT_STATE_ID) {
      // delete the current file while the file has snapshot feature
      if (!isCurrentFileDeleted()) {
        file.recordModification(priorSnapshotId);
        deleteCurrentFile();
      }
      collectBlocksAndClear(file, collectedBlocks, removedINodes);
      return Quota.Counts.newInstance();
    } else { // delete the snapshot
      priorSnapshotId = getDiffs().updatePrior(snapshotId, priorSnapshotId);
      return diffs.deleteSnapshotDiff(snapshotId, priorSnapshotId, file,
          collectedBlocks, removedINodes, countDiffChange);
    }
  }
  
  public void clearDiffs() {
    this.diffs.clear();
  }
  
  public Quota.Counts updateQuotaAndCollectBlocks(INodeFile file,
      FileDiff removed, BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes) {
    long oldDiskspace = file.diskspaceConsumed();
    if (removed.snapshotINode != null) {
      short replication = removed.snapshotINode.getFileReplication();
      short currentRepl = file.getBlockReplication();
      if (currentRepl == 0) {
        oldDiskspace = file.computeFileSize(true, true) * replication;
      } else if (replication > currentRepl) {  
        oldDiskspace = oldDiskspace / file.getBlockReplication() * replication;
      }
    }
    
    collectBlocksAndClear(file, collectedBlocks, removedINodes);
    
    long dsDelta = oldDiskspace - file.diskspaceConsumed();
    return Quota.Counts.newInstance(0, dsDelta);
  }
  
  /**
   * If some blocks at the end of the block list no longer belongs to
   * any inode, collect them and update the block list.
   */
  private void collectBlocksAndClear(final INodeFile file,
      final BlocksMapUpdateInfo info, final List<INode> removedINodes) {
    // check if everything is deleted.
    if (isCurrentFileDeleted() && getDiffs().asList().isEmpty()) {
      file.destroyAndCollectBlocks(info, removedINodes);
      return;
    }
    // find max file size.
    final long max;
    if (isCurrentFileDeleted()) {
      final FileDiff last = getDiffs().getLast();
      max = last == null? 0: last.getFileSize();
    } else { 
      max = file.computeFileSize();
    }

    collectBlocksBeyondMax(file, max, info);
  }

  private void collectBlocksBeyondMax(final INodeFile file, final long max,
      final BlocksMapUpdateInfo collectedBlocks) {
    final BlockInfo[] oldBlocks = file.getBlocks();
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
          newBlocks = BlockInfo.EMPTY_ARRAY;
        } else {
          newBlocks = new BlockInfo[n];
          System.arraycopy(oldBlocks, 0, newBlocks, 0, n);
        }
        
        // set new blocks
        file.setBlocks(newBlocks);

        // collect the blocks beyond max.  
        if (collectedBlocks != null) {
          for(; n < oldBlocks.length; n++) {
            collectedBlocks.addDeleteBlock(oldBlocks[n]);
          }
        }
      }
    }
  }
}
