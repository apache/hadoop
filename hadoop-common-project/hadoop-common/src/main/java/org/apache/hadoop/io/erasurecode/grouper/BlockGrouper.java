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
package org.apache.hadoop.io.erasurecode.grouper;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ECBlock;
import org.apache.hadoop.io.erasurecode.ECBlockGroup;
import org.apache.hadoop.io.erasurecode.ECSchema;

/**
 * As part of a codec, to handle how to form a block group for encoding
 * and provide instructions on how to recover erased blocks from a block group
 */
@InterfaceAudience.Private
public class BlockGrouper {

  private ECSchema schema;

  /**
   * Set EC schema.
   * @param schema
   */
  public void setSchema(ECSchema schema) {
    this.schema = schema;
  }

  /**
   * Get EC schema.
   * @return
   */
  protected ECSchema getSchema() {
    return schema;
  }

  /**
   * Get required data blocks count in a BlockGroup.
   * @return count of required data blocks
   */
  public int getRequiredNumDataBlocks() {
    return schema.getNumDataUnits();
  }

  /**
   * Get required parity blocks count in a BlockGroup.
   * @return count of required parity blocks
   */
  public int getRequiredNumParityBlocks() {
    return schema.getNumParityUnits();
  }

  /**
   * Calculating and organizing BlockGroup, to be called by ECManager
   * @param dataBlocks Data blocks to compute parity blocks against
   * @param parityBlocks To be computed parity blocks
   * @return
   */
  public ECBlockGroup makeBlockGroup(ECBlock[] dataBlocks,
                                     ECBlock[] parityBlocks) {

    ECBlockGroup blockGroup = new ECBlockGroup(dataBlocks, parityBlocks);
    return blockGroup;
  }

  /**
   * Given a BlockGroup, tell if any of the missing blocks can be recovered,
   * to be called by ECManager
   * @param blockGroup a blockGroup that may contain erased blocks but not sure
   *                   recoverable or not
   * @return true if any erased block recoverable, false otherwise
   */
  public boolean anyRecoverable(ECBlockGroup blockGroup) {
    int erasedCount = blockGroup.getErasedCount();

    return erasedCount > 0 && erasedCount <= getRequiredNumParityBlocks();
  }

}
