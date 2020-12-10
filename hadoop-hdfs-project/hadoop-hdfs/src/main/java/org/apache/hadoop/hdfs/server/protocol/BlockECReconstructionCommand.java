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

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;

import java.util.Arrays;
import java.util.Collection;

/**
 * A BlockECReconstructionCommand is an instruction to a DataNode to
 * reconstruct a striped block group with missing blocks.
 *
 * Upon receiving this command, the DataNode pulls data from other DataNodes
 * hosting blocks in this group and reconstructs the lost blocks through codec
 * calculation.
 *
 * After the reconstruction, the DataNode pushes the reconstructed blocks to
 * their final destinations if necessary (e.g., the destination is different
 * from the reconstruction node, or multiple blocks in a group are to be
 * reconstructed).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockECReconstructionCommand extends DatanodeCommand {
  private final Collection<BlockECReconstructionInfo> ecTasks;

  /**
   * Create BlockECReconstructionCommand from a collection of
   * {@link BlockECReconstructionInfo}, each representing a reconstruction
   * task
   */
  public BlockECReconstructionCommand(int action,
      Collection<BlockECReconstructionInfo> blockECReconstructionInfoList) {
    super(action);
    this.ecTasks = blockECReconstructionInfoList;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("BlockECReconstructionCommand(\n  ");
    Joiner.on("\n  ").appendTo(sb, ecTasks);
    sb.append("\n)");
    return sb.toString();
  }

  /** Block and targets pair */
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static class BlockECReconstructionInfo {
    private final ExtendedBlock block;
    private final DatanodeInfo[] sources;
    private DatanodeInfo[] targets;
    private String[] targetStorageIDs;
    private StorageType[] targetStorageTypes;
    private final byte[] liveBlockIndices;
    private final ErasureCodingPolicy ecPolicy;

    public BlockECReconstructionInfo(ExtendedBlock block,
        DatanodeInfo[] sources, DatanodeStorageInfo[] targetDnStorageInfo,
        byte[] liveBlockIndices, ErasureCodingPolicy ecPolicy) {
      this(block, sources, DatanodeStorageInfo
          .toDatanodeInfos(targetDnStorageInfo), DatanodeStorageInfo
          .toStorageIDs(targetDnStorageInfo), DatanodeStorageInfo
          .toStorageTypes(targetDnStorageInfo), liveBlockIndices, ecPolicy);
    }

    public BlockECReconstructionInfo(ExtendedBlock block,
        DatanodeInfo[] sources, DatanodeInfo[] targets,
        String[] targetStorageIDs, StorageType[] targetStorageTypes,
        byte[] liveBlockIndices, ErasureCodingPolicy ecPolicy) {
      this.block = block;
      this.sources = sources;
      this.targets = targets;
      this.targetStorageIDs = targetStorageIDs;
      this.targetStorageTypes = targetStorageTypes;
      this.liveBlockIndices = liveBlockIndices == null ?
          new byte[]{} : liveBlockIndices;
      this.ecPolicy = ecPolicy;
    }

    public ExtendedBlock getExtendedBlock() {
      return block;
    }

    public DatanodeInfo[] getSourceDnInfos() {
      return sources;
    }

    public DatanodeInfo[] getTargetDnInfos() {
      return targets;
    }

    public String[] getTargetStorageIDs() {
      return targetStorageIDs;
    }

    public StorageType[] getTargetStorageTypes() {
      return targetStorageTypes;
    }

    public byte[] getLiveBlockIndices() {
      return liveBlockIndices;
    }

    public ErasureCodingPolicy getErasureCodingPolicy() {
      return ecPolicy;
    }

    @Override
    public String toString() {
      return new StringBuilder().append("BlockECReconstructionInfo(\n  ")
          .append("Recovering ").append(block).append(" From: ")
          .append(Arrays.asList(sources)).append(" To: [")
          .append(Arrays.asList(targets)).append(")\n")
          .append(" Block Indices: ").append(Arrays.toString(liveBlockIndices))
          .toString();
    }
  }

  public Collection<BlockECReconstructionInfo> getECTasks() {
    return this.ecTasks;
  }
}
