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

/**
 * Type of a block. Previously, all blocks were replicated (contiguous).
 * Then Erasure Coded blocks (striped) were implemented.
 *
 * BlockTypes are currently defined by the highest bit in the block id. If
 * this bit is set, then the block is striped.
 *
 * Further extensions may claim the second bit s.t. the highest two bits are
 * set. e.g.
 * 0b00 == contiguous
 * 0b10 == striped
 * 0b11 == possible further extension block type.
 */
public enum BlockType {
  //! Replicated block.
  CONTIGUOUS,
  //! Erasure Coded Block
  STRIPED;

  // BLOCK_ID_MASK is the union of all masks.
  static final long BLOCK_ID_MASK          = 1L << 63;
  // BLOCK_ID_MASK_STRIPED is the mask for striped blocks.
  static final long BLOCK_ID_MASK_STRIPED  = 1L << 63;

  /**
   * Parse a BlockId to find the BlockType
   * Note: the old block id generation algorithm was based on a pseudo random
   * number generator, so there may be legacy blocks that make this conversion
   * unreliable.
   */
  public static BlockType fromBlockId(long blockId) {
    long blockType = blockId & BLOCK_ID_MASK;
    if(blockType == BLOCK_ID_MASK_STRIPED) {
      return STRIPED;
    }
    return CONTIGUOUS;
  }
}
