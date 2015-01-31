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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.util.SequentialNumber;

/**
 * Generate the next valid block group ID by incrementing the maximum block
 * group ID allocated so far, with the first 2^10 block group IDs reserved.
 * HDFS-EC introduces a hierarchical protocol to name blocks and groups:
 * Contiguous: {reserved block IDs | flag | block ID}
 * Striped: {reserved block IDs | flag | block group ID | index in group}
 *
 * Following n bits of reserved block IDs, The (n+1)th bit in an ID
 * distinguishes contiguous (0) and striped (1) blocks. For a striped block,
 * bits (n+2) to (64-m) represent the ID of its block group, while the last m
 * bits represent its index of the group. The value m is determined by the
 * maximum number of blocks in a group (MAX_BLOCKS_IN_GROUP).
 */
@InterfaceAudience.Private
public class SequentialBlockGroupIdGenerator extends SequentialNumber {

  private final BlockManager blockManager;

  SequentialBlockGroupIdGenerator(BlockManager blockManagerRef) {
    super(Long.MIN_VALUE);
    this.blockManager = blockManagerRef;
  }

  @Override // NumberGenerator
  public long nextValue() {
    // Skip to next legitimate block group ID based on the naming protocol
    while (super.getCurrentValue() % HdfsConstants.MAX_BLOCKS_IN_GROUP > 0) {
      super.nextValue();
    }
    // Make sure there's no conflict with existing random block IDs
    while (hasValidBlockInRange(super.getCurrentValue())) {
      super.skipTo(super.getCurrentValue() +
          HdfsConstants.MAX_BLOCKS_IN_GROUP);
    }
    if (super.getCurrentValue() >= 0) {
      BlockManager.LOG.warn("All negative block group IDs are used, " +
          "growing into positive IDs, " +
          "which might conflict with non-erasure coded blocks.");
    }
    return super.getCurrentValue();
  }

  /**
   *
   * @param id The starting ID of the range
   * @return true if any ID in the range
   *      {id, id+HdfsConstants.MAX_BLOCKS_IN_GROUP} is pointed-to by a file
   */
  private boolean hasValidBlockInRange(long id) {
    for (int i = 0; i < HdfsConstants.MAX_BLOCKS_IN_GROUP; i++) {
      Block b = new Block(id + i);
      if (blockManager.getBlockCollection(b) != null) {
        return true;
      }
    }
    return false;
  }
}
