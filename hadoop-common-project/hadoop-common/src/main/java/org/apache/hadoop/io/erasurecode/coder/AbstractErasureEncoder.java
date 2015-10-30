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
package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ECBlock;
import org.apache.hadoop.io.erasurecode.ECBlockGroup;
import org.apache.hadoop.io.erasurecode.ECSchema;

/**
 * An abstract erasure encoder that's to be inherited by new encoders.
 *
 * It implements the {@link ErasureCoder} interface.
 */
@InterfaceAudience.Private
public abstract class AbstractErasureEncoder extends AbstractErasureCoder {

  public AbstractErasureEncoder(int numDataUnits, int numParityUnits) {
    super(numDataUnits, numParityUnits);
  }

  public AbstractErasureEncoder(ECSchema schema) {
    super(schema);
  }

  @Override
  public ErasureCodingStep calculateCoding(ECBlockGroup blockGroup) {
    // We may have more than this when considering complicate cases. HADOOP-11550
    return prepareEncodingStep(blockGroup);
  }

  /**
   * Perform encoding against a block group.
   * @param blockGroup
   * @return encoding step for caller to do the real work
   */
  protected abstract ErasureCodingStep prepareEncodingStep(
      ECBlockGroup blockGroup);

  protected ECBlock[] getInputBlocks(ECBlockGroup blockGroup) {
    return blockGroup.getDataBlocks();
  }

  protected ECBlock[] getOutputBlocks(ECBlockGroup blockGroup) {
    return blockGroup.getParityBlocks();
  }
}
