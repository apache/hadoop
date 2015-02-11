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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.FSImageSerialization;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileAttributes;
import org.apache.hadoop.hdfs.server.namenode.QuotaCounts;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat.ReferenceMap;

/**
 * The difference of an {@link INodeFile} between two snapshots.
 */
public class FileDiff extends
    AbstractINodeDiff<INodeFile, INodeFileAttributes, FileDiff> {

  /** The file size at snapshot creation time. */
  private final long fileSize;
  /** A copy of the INodeFile block list. Used in truncate. */
  private BlockInfoContiguous[] blocks;

  FileDiff(int snapshotId, INodeFile file) {
    super(snapshotId, null, null);
    fileSize = file.computeFileSize();
    blocks = null;
  }

  /** Constructor used by FSImage loading */
  FileDiff(int snapshotId, INodeFileAttributes snapshotINode,
      FileDiff posteriorDiff, long fileSize) {
    super(snapshotId, snapshotINode, posteriorDiff);
    this.fileSize = fileSize;
    blocks = null;
  }

  /** @return the file size in the snapshot. */
  public long getFileSize() {
    return fileSize;
  }

  /**
   * Copy block references into the snapshot
   * up to the current {@link #fileSize}.
   * Should be done only once.
   */
  public void setBlocks(BlockInfoContiguous[] blocks) {
    if(this.blocks != null)
      return;
    int numBlocks = 0;
    for(long s = 0; numBlocks < blocks.length && s < fileSize; numBlocks++)
      s += blocks[numBlocks].getNumBytes();
    this.blocks = Arrays.copyOf(blocks, numBlocks);
  }

  public BlockInfoContiguous[] getBlocks() {
    return blocks;
  }

  @Override
  QuotaCounts combinePosteriorAndCollectBlocks(
      BlockStoragePolicySuite bsps, INodeFile currentINode,
      FileDiff posterior, BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes) {
    FileWithSnapshotFeature sf = currentINode.getFileWithSnapshotFeature();
    assert sf != null : "FileWithSnapshotFeature is null";
    return sf.updateQuotaAndCollectBlocks(
        bsps, currentINode, posterior, collectedBlocks, removedINodes);
  }
  
  @Override
  public String toString() {
    return super.toString() + " fileSize=" + fileSize + ", rep="
        + (snapshotINode == null? "?": snapshotINode.getFileReplication());
  }

  @Override
  void write(DataOutput out, ReferenceMap referenceMap) throws IOException {
    writeSnapshot(out);
    out.writeLong(fileSize);

    // write snapshotINode
    if (snapshotINode != null) {
      out.writeBoolean(true);
      FSImageSerialization.writeINodeFileAttributes(snapshotINode, out);
    } else {
      out.writeBoolean(false);
    }
  }

  @Override
  QuotaCounts destroyDiffAndCollectBlocks(BlockStoragePolicySuite bsps, INodeFile currentINode,
      BlocksMapUpdateInfo collectedBlocks, final List<INode> removedINodes) {
    return currentINode.getFileWithSnapshotFeature()
        .updateQuotaAndCollectBlocks(bsps, currentINode, this, collectedBlocks,
            removedINodes);
  }

  public void destroyAndCollectSnapshotBlocks(
      BlocksMapUpdateInfo collectedBlocks) {
    if(blocks == null || collectedBlocks == null)
      return;
    for(BlockInfoContiguous blk : blocks)
      collectedBlocks.addDeleteBlock(blk);
    blocks = null;
  }
}
