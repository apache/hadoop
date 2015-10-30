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
package org.apache.hadoop.io.erasurecode;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A group of blocks or {@link ECBlock} incurred in an erasure coding task.
 */
@InterfaceAudience.Private
public class ECBlockGroup {

  private ECBlock[] dataBlocks;
  private ECBlock[] parityBlocks;

  /**
   * A constructor specifying data blocks and parity blocks.
   * @param dataBlocks data blocks in the group
   * @param parityBlocks parity blocks in the group
   */
  public ECBlockGroup(ECBlock[] dataBlocks, ECBlock[] parityBlocks) {
    this.dataBlocks = dataBlocks;
    this.parityBlocks = parityBlocks;
  }

  /**
   * Get data blocks
   * @return data blocks
   */
  public ECBlock[] getDataBlocks() {
    return dataBlocks;
  }

  /**
   * Get parity blocks
   * @return parity blocks
   */
  public ECBlock[] getParityBlocks() {
    return parityBlocks;
  }

  /**
   * Any erased data block?
   * @return true if any erased data block, false otherwise
   */
  public boolean anyErasedDataBlock() {
    for (int i = 0; i < dataBlocks.length; ++i) {
      if (dataBlocks[i].isErased()) {
        return true;
      }
    }

    return false;
  }

  /**
   * Any erased parity block?
   * @return true if any erased parity block, false otherwise
   */
  public boolean anyErasedParityBlock() {
    for (int i = 0; i < parityBlocks.length; ++i) {
      if (parityBlocks[i].isErased()) {
        return true;
      }
    }

    return false;
  }

  /**
   * Get erased blocks count
   * @return erased count of blocks
   */
  public int getErasedCount() {
    int erasedCount = 0;

    for (ECBlock dataBlock : dataBlocks) {
      if (dataBlock.isErased()) erasedCount++;
    }

    for (ECBlock parityBlock : parityBlocks) {
      if (parityBlock.isErased()) erasedCount++;
    }

    return erasedCount;
  }

}
