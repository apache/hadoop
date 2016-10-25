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

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/**
 * A BlockStorageMovementCommand is an instruction to a DataNode to move the
 * given set of blocks to specified target DataNodes to fulfill the block
 * storage policy.
 *
 * Upon receiving this command, this DataNode coordinates all the block movement
 * by passing the details to
 * {@link org.apache.hadoop.hdfs.server.datanode.StoragePolicySatisfyWorker}
 * service. After the block movement this DataNode sends response back to the
 * NameNode about the movement status.
 *
 * The coordinator datanode will use 'trackId' identifier to coordinate the
 * block movement of the given set of blocks. TrackId is a unique identifier
 * that represents a group of blocks. Namenode will generate this unique value
 * and send it to the coordinator datanode along with the
 * BlockStorageMovementCommand. Datanode will monitor the completion of the
 * block movements that grouped under this trackId and notifies Namenode about
 * the completion status.
 */
public class BlockStorageMovementCommand extends DatanodeCommand {
  private final long trackID;
  private final String blockPoolId;
  private final Collection<BlockMovingInfo> blockMovingTasks;

  /**
   * Block storage movement command constructor.
   *
   * @param action
   *          protocol specific action
   * @param trackID
   *          unique identifier to monitor the given set of block movements
   * @param blockPoolId
   *          block pool ID
   * @param blockMovingInfos
   *          block to storage info that will be used for movement
   */
  public BlockStorageMovementCommand(int action, long trackID,
      String blockPoolId, Collection<BlockMovingInfo> blockMovingInfos) {
    super(action);
    this.trackID = trackID;
    this.blockPoolId = blockPoolId;
    this.blockMovingTasks = blockMovingInfos;
  }

  /**
   * Returns trackID, which will be used to monitor the block movement assigned
   * to this coordinator datanode.
   */
  public long getTrackID() {
    return trackID;
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
    private DatanodeInfo[] sourceNodes;
    private DatanodeInfo[] targetNodes;
    private StorageType[] sourceStorageTypes;
    private StorageType[] targetStorageTypes;

    /**
     * Block to storage info constructor.
     *
     * @param block
     *          block
     * @param sourceDnInfos
     *          node that can be the sources of a block move
     * @param targetDnInfos
     *          target datanode info
     * @param srcStorageTypes
     *          type of source storage media
     * @param targetStorageTypes
     *          type of destin storage media
     */
    public BlockMovingInfo(Block block,
        DatanodeInfo[] sourceDnInfos, DatanodeInfo[] targetDnInfos,
        StorageType[] srcStorageTypes, StorageType[] targetStorageTypes) {
      this.blk = block;
      this.sourceNodes = sourceDnInfos;
      this.targetNodes = targetDnInfos;
      this.sourceStorageTypes = srcStorageTypes;
      this.targetStorageTypes = targetStorageTypes;
    }

    public void addBlock(Block block) {
      this.blk = block;
    }

    public Block getBlock() {
      return this.blk;
    }

    public DatanodeInfo[] getSources() {
      return sourceNodes;
    }

    public DatanodeInfo[] getTargets() {
      return targetNodes;
    }

    public StorageType[] getTargetStorageTypes() {
      return targetStorageTypes;
    }

    public StorageType[] getSourceStorageTypes() {
      return sourceStorageTypes;
    }

    @Override
    public String toString() {
      return new StringBuilder().append("BlockMovingInfo(\n  ")
          .append("Moving block: ").append(blk).append(" From: ")
          .append(Arrays.asList(sourceNodes)).append(" To: [")
          .append(Arrays.asList(targetNodes)).append("\n  ")
          .append(" sourceStorageTypes: ")
          .append(Arrays.toString(sourceStorageTypes))
          .append(" targetStorageTypes: ")
          .append(Arrays.toString(targetStorageTypes)).append(")").toString();
    }
  }
}
