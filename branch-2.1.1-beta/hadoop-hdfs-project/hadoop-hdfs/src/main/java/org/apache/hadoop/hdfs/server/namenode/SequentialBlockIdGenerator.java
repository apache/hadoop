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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.util.SequentialNumber;

/**
 * Generate the next valid block ID by incrementing the maximum block
 * ID allocated so far, starting at 2^30+1.
 *
 * Block IDs used to be allocated randomly in the past. Hence we may
 * find some conflicts while stepping through the ID space sequentially.
 * However given the sparsity of the ID space, conflicts should be rare
 * and can be skipped over when detected.
 */
@InterfaceAudience.Private
public class SequentialBlockIdGenerator extends SequentialNumber {
  /**
   * The last reserved block ID.
   */
  public static final long LAST_RESERVED_BLOCK_ID = 1024L * 1024 * 1024;

  private final BlockManager blockManager;

  SequentialBlockIdGenerator(BlockManager blockManagerRef) {
    super(LAST_RESERVED_BLOCK_ID);
    this.blockManager = blockManagerRef;
  }

  @Override // NumberGenerator
  public long nextValue() {
    Block b = new Block(super.nextValue());

    // There may be an occasional conflict with randomly generated
    // block IDs. Skip over the conflicts.
    while(isValidBlock(b)) {
      b.setBlockId(super.nextValue());
    }
    return b.getBlockId();
  }

  /**
   * Returns whether the given block is one pointed-to by a file.
   */
  private boolean isValidBlock(Block b) {
    return (blockManager.getBlockCollection(b) != null);
  }
}
