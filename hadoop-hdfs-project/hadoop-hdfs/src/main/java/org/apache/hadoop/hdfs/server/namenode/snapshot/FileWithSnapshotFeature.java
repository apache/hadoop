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

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.AclFeature;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.AclStorage;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileAttributes;
import org.apache.hadoop.hdfs.server.namenode.QuotaCounts;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.util.EnumCounters;

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
  
  public QuotaCounts cleanFile(final BlockStoragePolicySuite bsps,
      final INodeFile file, final int snapshotId,
      int priorSnapshotId, final BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes) {
    if (snapshotId == Snapshot.CURRENT_STATE_ID) {
      // delete the current file while the file has snapshot feature
      if (!isCurrentFileDeleted()) {
        file.recordModification(priorSnapshotId);
        deleteCurrentFile();
      }
      collectBlocksAndClear(bsps, file, collectedBlocks, removedINodes);
      return new QuotaCounts.Builder().build();
    } else { // delete the snapshot
      priorSnapshotId = getDiffs().updatePrior(snapshotId, priorSnapshotId);
      return diffs.deleteSnapshotDiff(bsps, snapshotId, priorSnapshotId, file,
          collectedBlocks, removedINodes);
    }
  }
  
  public void clearDiffs() {
    this.diffs.clear();
  }
  
  public QuotaCounts updateQuotaAndCollectBlocks(BlockStoragePolicySuite bsps, INodeFile file,
      FileDiff removed, BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes) {
    long oldStoragespace = file.storagespaceConsumed();

    byte storagePolicyID = file.getStoragePolicyID();
    BlockStoragePolicy bsp = null;
    EnumCounters<StorageType> typeSpaces =
        new EnumCounters<StorageType>(StorageType.class);
    if (storagePolicyID != BlockStoragePolicySuite.ID_UNSPECIFIED) {
      bsp = bsps.getPolicy(file.getStoragePolicyID());
    }

    if (removed.snapshotINode != null) {
      short replication = removed.snapshotINode.getFileReplication();
      short currentRepl = file.getBlockReplication();
      if (currentRepl == 0) {
        long oldFileSizeNoRep = file.computeFileSize(true, true);
        oldStoragespace =  oldFileSizeNoRep * replication;

        if (bsp != null) {
          List<StorageType> oldTypeChosen = bsp.chooseStorageTypes(replication);
          for (StorageType t : oldTypeChosen) {
            if (t.supportTypeQuota()) {
              typeSpaces.add(t, -oldFileSizeNoRep);
            }
          }
        }
      } else if (replication > currentRepl) {
        long oldFileSizeNoRep = file.storagespaceConsumedNoReplication();
        oldStoragespace = oldFileSizeNoRep * replication;

        if (bsp != null) {
          List<StorageType> oldTypeChosen = bsp.chooseStorageTypes(replication);
          for (StorageType t : oldTypeChosen) {
            if (t.supportTypeQuota()) {
              typeSpaces.add(t, -oldFileSizeNoRep);
            }
          }
          List<StorageType> newTypeChosen = bsp.chooseStorageTypes(currentRepl);
          for (StorageType t: newTypeChosen) {
            if (t.supportTypeQuota()) {
              typeSpaces.add(t, oldFileSizeNoRep);
            }
          }
        }
      }
      AclFeature aclFeature = removed.getSnapshotINode().getAclFeature();
      if (aclFeature != null) {
        AclStorage.removeAclFeature(aclFeature);
      }
    }

    getDiffs().combineAndCollectSnapshotBlocks(
        bsps, file, removed, collectedBlocks, removedINodes);

    long ssDelta = oldStoragespace - file.storagespaceConsumed();
    return new QuotaCounts.Builder().
        storageSpace(ssDelta).
        typeSpaces(typeSpaces).
        build();
  }

  /**
   * If some blocks at the end of the block list no longer belongs to
   * any inode, collect them and update the block list.
   */
  public void collectBlocksAndClear(final BlockStoragePolicySuite bsps, final INodeFile file,
      final BlocksMapUpdateInfo info, final List<INode> removedINodes) {
    // check if everything is deleted.
    if (isCurrentFileDeleted() && getDiffs().asList().isEmpty()) {
      file.destroyAndCollectBlocks(bsps, info, removedINodes);
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
    BlockInfoContiguous[] snapshotBlocks = last == null ? null : last.getBlocks();
    if(snapshotBlocks == null)
      file.collectBlocksBeyondMax(max, info);
    else
      file.collectBlocksBeyondSnapshot(snapshotBlocks, info);
  }
}
