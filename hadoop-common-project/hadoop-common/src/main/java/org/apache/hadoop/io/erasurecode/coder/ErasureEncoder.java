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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.erasurecode.ECBlock;
import org.apache.hadoop.io.erasurecode.ECBlockGroup;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;

/**
 * An abstract erasure encoder that's to be inherited by new encoders.
 *
 * It implements the {@link ErasureCoder} interface.
 */
@InterfaceAudience.Private
public abstract class ErasureEncoder extends Configured
    implements ErasureCoder {

  private final int numDataUnits;
  private final int numParityUnits;
  private final ErasureCoderOptions options;

  public ErasureEncoder(ErasureCoderOptions options) {
    this.options = options;
    this.numDataUnits = options.getNumDataUnits();
    this.numParityUnits = options.getNumParityUnits();
  }

  @Override
  public ErasureCodingStep calculateCoding(ECBlockGroup blockGroup) {
    // We may have more than this when considering complicate cases. HADOOP-11550
    return prepareEncodingStep(blockGroup);
  }

  @Override
  public int getNumDataUnits() {
    return numDataUnits;
  }

  @Override
  public int getNumParityUnits() {
    return numParityUnits;
  }

  @Override
  public ErasureCoderOptions getOptions() {
    return options;
  }

  protected ECBlock[] getInputBlocks(ECBlockGroup blockGroup) {
    return blockGroup.getDataBlocks();
  }

  protected ECBlock[] getOutputBlocks(ECBlockGroup blockGroup) {
    return blockGroup.getParityBlocks();
  }

  @Override
  public boolean preferDirectBuffer() {
    return false;
  }

  @Override
  public void release() {
    // Nothing to do by default
  }

  /**
   * Perform encoding against a block group.
   * @param blockGroup
   * @return encoding step for caller to do the real work
   */
  protected abstract ErasureCodingStep prepareEncodingStep(
      ECBlockGroup blockGroup);
}
