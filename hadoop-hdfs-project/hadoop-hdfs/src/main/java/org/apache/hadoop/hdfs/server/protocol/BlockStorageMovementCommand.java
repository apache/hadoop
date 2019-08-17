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
package org.apache.hadoop.hdfs.server.protocol;

import java.util.Collection;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/**
 * A BlockStorageMovementCommand is an instruction to a DataNode to move the
 * given set of blocks to specified target DataNodes to fulfill the block
 * storage policy.
 *
 * Upon receiving this command, this DataNode pass the array of block movement
 * details to
 * {@link org.apache.hadoop.hdfs.server.sps.ExternalSPSBlockMoveTaskHandler}
 * service. Later, ExternalSPSBlockMoveTaskHandler will schedule block movement
 * tasks for these blocks and monitors the completion of each task. After the
 * block movement attempt is finished(with success or failure) this DataNode
 * will send response back to NameNode about the block movement attempt
 * finished details.
 */
public class BlockStorageMovementCommand extends DatanodeCommand {
  private final String blockPoolId;
  private final Collection<BlockMovingInfo> blockMovingTasks;

  /**
   * Block storage movement command constructor.
   *
   * @param action
   *          protocol specific action
   * @param blockMovingInfos
   *          block to storage info that will be used for movement
   */
  public BlockStorageMovementCommand(int action, String blockPoolId,
      Collection<BlockMovingInfo> blockMovingInfos) {
    super(action);
    this.blockPoolId = blockPoolId;
    this.blockMovingTasks = blockMovingInfos;
  }

  /**
   * Returns block pool ID.
   */
  public String getBlockPoolId() {
    return blockPoolId;
  }

  /**
   * Returns the list of blocks to be moved.
   */
  public Collection<BlockMovingInfo> getBlockMovingTasks() {
    return blockMovingTasks;
  }

  /**
   * Stores block to storage info that can be used for block movement.
   */
  public static class BlockMovingInfo {
    private Block blk;
    private DatanodeInfo sourceNode;
    private DatanodeInfo targetNode;
    private StorageType sourceStorageType;
    private StorageType targetStorageType;

    /**
     * Block to storage info constructor.
     *
     * @param block
     *          block info
     * @param sourceDnInfo
     *          node that can be the source of a block move
     * @param srcStorageType
     *          type of source storage media
     */
    public BlockMovingInfo(Block block, DatanodeInfo sourceDnInfo,
        DatanodeInfo targetDnInfo, StorageType srcStorageType,
        StorageType targetStorageType) {
      this.blk = block;
      this.sourceNode = sourceDnInfo;
      this.targetNode = targetDnInfo;
      this.sourceStorageType = srcStorageType;
      this.targetStorageType = targetStorageType;
    }

    public void addBlock(Block block) {
      this.blk = block;
    }

    public Block getBlock() {
      return blk;
    }

    public DatanodeInfo getSource() {
      return sourceNode;
    }

    public DatanodeInfo getTarget() {
      return targetNode;
    }

    public StorageType getTargetStorageType() {
      return targetStorageType;
    }

    public StorageType getSourceStorageType() {
      return sourceStorageType;
    }

    @Override
    public String toString() {
      return new StringBuilder().append("BlockMovingInfo(\n  ")
          .append("Moving block: ").append(blk).append(" From: ")
          .append(sourceNode).append(" To: [").append(targetNode).append("\n  ")
          .append(" sourceStorageType: ").append(sourceStorageType)
          .append(" targetStorageType: ").append(targetStorageType).append(")")
          .toString();
    }
  }
}
