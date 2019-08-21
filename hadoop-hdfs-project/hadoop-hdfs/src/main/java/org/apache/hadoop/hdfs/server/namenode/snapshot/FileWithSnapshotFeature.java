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
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.AclFeature;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.AclStorage;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileAttributes;
import org.apache.hadoop.hdfs.server.namenode.QuotaCounts;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;

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
  public short getMaxBlockRepInDiffs(FileDiff excluded) {
    short max = 0;
    for(FileDiff d : getDiffs()) {
      if (d != excluded && d.snapshotINode != null) {
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

    final DiffList<FileDiff> diffList = diffs.asList();
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
  
  public void cleanFile(INode.ReclaimContext reclaimContext,
      final INodeFile file, final int snapshotId, int priorSnapshotId,
      byte storagePolicyId) {
    if (snapshotId == Snapshot.CURRENT_STATE_ID) {
      // delete the current file while the file has snapshot feature
      if (!isCurrentFileDeleted()) {
        file.recordModification(priorSnapshotId);
        deleteCurrentFile();
      }
      final BlockStoragePolicy policy = reclaimContext.storagePolicySuite()
          .getPolicy(storagePolicyId);
      QuotaCounts old = file.storagespaceConsumed(policy);
      collectBlocksAndClear(reclaimContext, file);
      QuotaCounts current = file.storagespaceConsumed(policy);
      reclaimContext.quotaDelta().add(old.subtract(current));
    } else { // delete the snapshot
      priorSnapshotId = getDiffs().updatePrior(snapshotId, priorSnapshotId);
      diffs.deleteSnapshotDiff(reclaimContext, snapshotId, priorSnapshotId,
          file);
    }
  }
  
  public void clearDiffs() {
    this.diffs.clear();
  }
  
  public void updateQuotaAndCollectBlocks(INode.ReclaimContext reclaimContext,
      INodeFile file, FileDiff removed) {
    byte storagePolicyID = file.getStoragePolicyID();
    BlockStoragePolicy bsp = null;
    if (storagePolicyID != HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
      bsp = reclaimContext.storagePolicySuite().
          getPolicy(file.getStoragePolicyID());
    }

    QuotaCounts oldCounts;
    if (removed.snapshotINode != null) {
      oldCounts = new QuotaCounts.Builder().build();
      BlockInfo[] blocks = file.getBlocks() == null ? new
          BlockInfo[0] : file.getBlocks();
      for (BlockInfo b: blocks) {
        short replication = b.getReplication();
        long blockSize = b.isComplete() ? b.getNumBytes() : file
            .getPreferredBlockSize();

        oldCounts.addStorageSpace(blockSize * replication);

        if (bsp != null) {
          List<StorageType> oldTypeChosen = bsp.chooseStorageTypes(replication);
          for (StorageType t : oldTypeChosen) {
            if (t.supportTypeQuota()) {
              oldCounts.addTypeSpace(t, blockSize);
            }
          }
        }
      }

      AclFeature aclFeature = removed.getSnapshotINode().getAclFeature();
      if (aclFeature != null) {
        AclStorage.removeAclFeature(aclFeature);
      }
    } else {
      oldCounts = file.storagespaceConsumed(null);
    }

    getDiffs().combineAndCollectSnapshotBlocks(reclaimContext, file, removed);
    if (file.getBlocks() != null) {
      short replInDiff = getMaxBlockRepInDiffs(removed);
      short repl = (short) Math.max(file.getPreferredBlockReplication(),
                                    replInDiff);
      for (BlockInfo b : file.getBlocks()) {
        if (repl != b.getReplication()) {
          reclaimContext.collectedBlocks().addUpdateReplicationFactor(b, repl);
        }
      }
    }
    QuotaCounts current = file.storagespaceConsumed(bsp);
    reclaimContext.quotaDelta().add(oldCounts.subtract(current));
  }

  /**
   * If some blocks at the end of the block list no longer belongs to
   * any inode, collect them and update the block list.
   */
  public void collectBlocksAndClear(
      INode.ReclaimContext reclaimContext, final INodeFile file) {
    // check if everything is deleted.
    if (isCurrentFileDeleted() && getDiffs().asList().isEmpty()) {
      file.clearFile(reclaimContext);
      return;
    }
    // find max file size.
    final long max;
    FileDiff diff = getDiffs().getLast();
    if (isCurrentFileDeleted()) {
      max = diff == null? 0: diff.getFileSize();
    } else {
      max = file.computeFileSize();
    }

    // Collect blocks that should be deleted
    FileDiff last = diffs.getLast();
    BlockInfo[] snapshotBlocks = last == null ? null : last.getBlocks();
    if(snapshotBlocks == null)
      file.collectBlocksBeyondMax(max, reclaimContext.collectedBlocks(), null);
    else
      file.collectBlocksBeyondSnapshot(snapshotBlocks,
                                       reclaimContext.collectedBlocks());
  }

  @Override
  public String toString() {
    return "" + diffs;
  }
}
