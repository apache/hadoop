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

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

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
 */
public class BlockStorageMovementCommand extends DatanodeCommand {

  // TODO: constructor needs to be refined based on the block movement data
  // structure.
  BlockStorageMovementCommand(int action) {
    super(action);
  }

  /**
   * Stores block to storage info that can be used for block movement.
   */
  public static class BlockMovingInfo {
    private ExtendedBlock blk;
    private DatanodeInfo[] sourceNodes;
    private StorageType[] sourceStorageTypes;
    private DatanodeInfo[] targetNodes;
    private StorageType[] targetStorageTypes;

    public BlockMovingInfo(ExtendedBlock block,
        DatanodeInfo[] sourceDnInfos, DatanodeInfo[] targetDnInfos,
        StorageType[] srcStorageTypes, StorageType[] targetStorageTypes) {
      this.blk = block;
      this.sourceNodes = sourceDnInfos;
      this.targetNodes = targetDnInfos;
      this.sourceStorageTypes = srcStorageTypes;
      this.targetStorageTypes = targetStorageTypes;
    }

    public void addBlock(ExtendedBlock block) {
      this.blk = block;
    }

    public ExtendedBlock getBlock() {
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
          .append(Arrays.asList(targetNodes)).append(")\n")
          .append(" sourceStorageTypes: ")
          .append(Arrays.toString(sourceStorageTypes))
          .append(" targetStorageTypes: ")
          .append(Arrays.toString(targetStorageTypes)).toString();
    }
  }
}
