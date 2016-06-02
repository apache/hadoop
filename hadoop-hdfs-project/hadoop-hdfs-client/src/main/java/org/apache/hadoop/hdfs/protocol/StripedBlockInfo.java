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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

/**
 * Striped block info that can be sent elsewhere to do block group level things,
 * like checksum, and etc.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class StripedBlockInfo {
  private final ExtendedBlock block;
  private final DatanodeInfo[] datanodes;
  private final Token<BlockTokenIdentifier>[] blockTokens;
  private final byte[] blockIndices;
  private final ErasureCodingPolicy ecPolicy;

  public StripedBlockInfo(ExtendedBlock block, DatanodeInfo[] datanodes,
      Token<BlockTokenIdentifier>[] blockTokens, byte[] blockIndices,
      ErasureCodingPolicy ecPolicy) {
    this.block = block;
    this.datanodes = datanodes;
    this.blockTokens = blockTokens;
    this.blockIndices = blockIndices;
    this.ecPolicy = ecPolicy;
  }

  public ExtendedBlock getBlock() {
    return block;
  }

  public DatanodeInfo[] getDatanodes() {
    return datanodes;
  }

  public Token<BlockTokenIdentifier>[] getBlockTokens() {
    return blockTokens;
  }

  public byte[] getBlockIndices() {
    return blockIndices;
  }

  public ErasureCodingPolicy getErasureCodingPolicy() {
    return ecPolicy;
  }
}
