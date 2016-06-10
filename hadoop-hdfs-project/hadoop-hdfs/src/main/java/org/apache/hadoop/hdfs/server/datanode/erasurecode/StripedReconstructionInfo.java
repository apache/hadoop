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
package org.apache.hadoop.hdfs.server.datanode.erasurecode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

/**
 * Stores striped block info that can be used for block reconstruction.
 */
@InterfaceAudience.Private
public class StripedReconstructionInfo {

  private final ExtendedBlock blockGroup;
  private final ErasureCodingPolicy ecPolicy;

  // source info
  private final byte[] liveIndices;
  private final DatanodeInfo[] sources;

  // target info
  private final byte[] targetIndices;
  private final DatanodeInfo[] targets;
  private final StorageType[] targetStorageTypes;

  public StripedReconstructionInfo(ExtendedBlock blockGroup,
      ErasureCodingPolicy ecPolicy, byte[] liveIndices, DatanodeInfo[] sources,
      byte[] targetIndices) {
    this(blockGroup, ecPolicy, liveIndices, sources, targetIndices, null, null);
  }

  StripedReconstructionInfo(ExtendedBlock blockGroup,
      ErasureCodingPolicy ecPolicy, byte[] liveIndices, DatanodeInfo[] sources,
      DatanodeInfo[] targets, StorageType[] targetStorageTypes) {
    this(blockGroup, ecPolicy, liveIndices, sources, null, targets,
        targetStorageTypes);
  }

  private StripedReconstructionInfo(ExtendedBlock blockGroup,
      ErasureCodingPolicy ecPolicy, byte[] liveIndices, DatanodeInfo[] sources,
      byte[] targetIndices, DatanodeInfo[] targets,
      StorageType[] targetStorageTypes) {

    this.blockGroup = blockGroup;
    this.ecPolicy = ecPolicy;
    this.liveIndices = liveIndices;
    this.sources = sources;
    this.targetIndices = targetIndices;
    this.targets = targets;
    this.targetStorageTypes = targetStorageTypes;
  }

  ExtendedBlock getBlockGroup() {
    return blockGroup;
  }

  ErasureCodingPolicy getEcPolicy() {
    return ecPolicy;
  }

  byte[] getLiveIndices() {
    return liveIndices;
  }

  DatanodeInfo[] getSources() {
    return sources;
  }

  byte[] getTargetIndices() {
    return targetIndices;
  }

  DatanodeInfo[] getTargets() {
    return targets;
  }

  StorageType[] getTargetStorageTypes() {
    return targetStorageTypes;
  }
}

