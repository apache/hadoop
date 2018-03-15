/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.common;

import org.apache.hadoop.hdsl.protocol.proto
    .ScmBlockLocationProtocolProtos.KeyBlocks;

import java.util.List;

/**
 * A group of blocks relations relevant, e.g belong to a certain object key.
 */
public final class BlockGroup {

  private String groupID;
  private List<String> blockIDs;
  private BlockGroup(String groupID, List<String> blockIDs) {
    this.groupID = groupID;
    this.blockIDs = blockIDs;
  }

  public List<String> getBlockIDList() {
    return blockIDs;
  }

  public String getGroupID() {
    return groupID;
  }

  public KeyBlocks getProto() {
    return KeyBlocks.newBuilder().setKey(groupID)
        .addAllBlocks(blockIDs).build();
  }

  /**
   * Parses a KeyBlocks proto to a group of blocks.
   * @param proto KeyBlocks proto.
   * @return a group of blocks.
   */
  public static BlockGroup getFromProto(KeyBlocks proto) {
    return BlockGroup.newBuilder().setKeyName(proto.getKey())
        .addAllBlockIDs(proto.getBlocksList()).build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * BlockGroup instance builder.
   */
  public static class Builder {

    private String groupID;
    private List<String> blockIDs;

    public Builder setKeyName(String blockGroupID) {
      this.groupID = blockGroupID;
      return this;
    }

    public Builder addAllBlockIDs(List<String> keyBlocks) {
      this.blockIDs = keyBlocks;
      return this;
    }

    public BlockGroup build() {
      return new BlockGroup(groupID, blockIDs);
    }
  }

}
