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
import org.apache.hadoop.util.SequentialNumber;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BLOCK_GROUP_INDEX_MASK;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.MAX_BLOCKS_IN_GROUP;

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
 *
 * Note that the {@link #nextValue()} methods requires external lock to
 * guarantee IDs have no conflicts.
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
    skipTo((getCurrentValue() & ~BLOCK_GROUP_INDEX_MASK) + MAX_BLOCKS_IN_GROUP);
    // Make sure there's no conflict with existing random block IDs
    final Block b = new Block(getCurrentValue());
    while (hasValidBlockInRange(b)) {
      skipTo(getCurrentValue() + MAX_BLOCKS_IN_GROUP);
      b.setBlockId(getCurrentValue());
    }
    if (b.getBlockId() >= 0) {
      throw new IllegalStateException("All negative block group IDs are used, "
          + "growing into positive IDs, "
          + "which might conflict with non-erasure coded blocks.");
    }
    return getCurrentValue();
  }

  /**
   * @param b A block object whose id is set to the starting point for check
   * @return true if any ID in the range
   *      {id, id+HdfsConstants.MAX_BLOCKS_IN_GROUP} is pointed-to by a stored
   *      block.
   */
  private boolean hasValidBlockInRange(Block b) {
    final long id = b.getBlockId();
    for (int i = 0; i < MAX_BLOCKS_IN_GROUP; i++) {
      b.setBlockId(id + i);
      if (blockManager.getStoredBlock(b) != null) {
        return true;
      }
    }
    return false;
  }
}
