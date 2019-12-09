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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class LocatedBlockBuilder {

  protected long flen;
  protected List<LocatedBlock> blocks = Collections.<LocatedBlock>emptyList();
  protected boolean isUC;
  protected LocatedBlock last;
  protected boolean lastComplete;
  protected FileEncryptionInfo feInfo;
  private final int maxBlocks;
  protected ErasureCodingPolicy ecPolicy;

  LocatedBlockBuilder(int maxBlocks) {
    this.maxBlocks = maxBlocks;
  }

  boolean isBlockMax() {
    return blocks.size() >= maxBlocks;
  }

  LocatedBlockBuilder fileLength(long fileLength) {
    flen = fileLength;
    return this;
  }

  LocatedBlockBuilder addBlock(LocatedBlock block) {
    if (blocks.isEmpty()) {
      blocks = new ArrayList<>();
    }
    blocks.add(block);
    return this;
  }

  // return new block so tokens can be set
  LocatedBlock newLocatedBlock(ExtendedBlock eb,
      DatanodeStorageInfo[] storage,
      long pos, boolean isCorrupt) {
    LocatedBlock blk =
        BlockManager.newLocatedBlock(eb, storage, pos, isCorrupt);
    return blk;
  }

  LocatedBlockBuilder lastUC(boolean underConstruction) {
    isUC = underConstruction;
    return this;
  }

  LocatedBlockBuilder lastBlock(LocatedBlock block) {
    last = block;
    return this;
  }

  LocatedBlockBuilder lastComplete(boolean complete) {
    lastComplete = complete;
    return this;
  }

  LocatedBlockBuilder encryption(FileEncryptionInfo fileEncryptionInfo) {
    feInfo = fileEncryptionInfo;
    return this;
  }

  LocatedBlockBuilder erasureCoding(ErasureCodingPolicy codingPolicy) {
    ecPolicy = codingPolicy;
    return this;
  }

  LocatedBlocks build(DatanodeDescriptor client) {
    return build();
  }

  LocatedBlocks build() {
    return new LocatedBlocks(flen, isUC, blocks, last,
        lastComplete, feInfo, ecPolicy);
  }

}
