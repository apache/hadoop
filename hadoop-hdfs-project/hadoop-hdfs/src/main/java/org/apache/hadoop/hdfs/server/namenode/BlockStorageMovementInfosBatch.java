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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.List;

import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;

/**
 * This class represents a batch of blocks under one trackId which needs to move
 * its storage locations to satisfy the storage policy.
 */
public class BlockStorageMovementInfosBatch {
  private long trackID;
  private List<BlockMovingInfo> blockMovingInfos;

  /**
   * Constructor to create the block storage movement infos batch.
   *
   * @param trackID
   *          - unique identifier which will be used for tracking the given set
   *          of blocks movement.
   * @param blockMovingInfos
   *          - list of block to storage infos.
   */
  public BlockStorageMovementInfosBatch(long trackID,
      List<BlockMovingInfo> blockMovingInfos) {
    this.trackID = trackID;
    this.blockMovingInfos = blockMovingInfos;
  }

  public long getTrackID() {
    return trackID;
  }

  public List<BlockMovingInfo> getBlockMovingInfo() {
    return blockMovingInfos;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("BlockStorageMovementInfosBatch(\n  ")
        .append("TrackID: ").append(trackID).append("  BlockMovingInfos: ")
        .append(blockMovingInfos).append(")").toString();
  }
}
