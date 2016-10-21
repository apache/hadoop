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
 * An abstract erasure decoder that's to be inherited by new decoders.
 *
 * It implements the {@link ErasureCoder} interface.
 */
@InterfaceAudience.Private
public abstract class ErasureDecoder extends Configured
    implements ErasureCoder {
  private final int numDataUnits;
  private final int numParityUnits;
  private final ErasureCoderOptions options;

  public ErasureDecoder(ErasureCoderOptions options) {
    this.options = options;
    this.numDataUnits = options.getNumDataUnits();
    this.numParityUnits = options.getNumParityUnits();
  }

  @Override
  public ErasureCodingStep calculateCoding(ECBlockGroup blockGroup) {
    // We may have more than this when considering complicate cases. HADOOP-11550
    return prepareDecodingStep(blockGroup);
  }

  @Override
  public int getNumDataUnits() {
    return this.numDataUnits;
  }

  @Override
  public int getNumParityUnits() {
    return this.numParityUnits;
  }

  @Override
  public ErasureCoderOptions getOptions() {
    return options;
  }

  /**
   * We have all the data blocks and parity blocks as input blocks for
   * recovering by default. It's codec specific
   * @param blockGroup
   * @return input blocks
   */
  protected ECBlock[] getInputBlocks(ECBlockGroup blockGroup) {
    ECBlock[] inputBlocks = new ECBlock[getNumDataUnits() +
            getNumParityUnits()];

    System.arraycopy(blockGroup.getDataBlocks(), 0, inputBlocks,
            0, getNumDataUnits());

    System.arraycopy(blockGroup.getParityBlocks(), 0, inputBlocks,
            getNumDataUnits(), getNumParityUnits());

    return inputBlocks;
  }

  /**
   * Which blocks were erased ?
   * @param blockGroup
   * @return output blocks to recover
   */
  protected ECBlock[] getOutputBlocks(ECBlockGroup blockGroup) {
    ECBlock[] outputBlocks = new ECBlock[getNumErasedBlocks(blockGroup)];

    int idx = 0;

    for (int i = 0; i < getNumDataUnits(); i++) {
      if (blockGroup.getDataBlocks()[i].isErased()) {
        outputBlocks[idx++] = blockGroup.getDataBlocks()[i];
      }
    }

    for (int i = 0; i < getNumParityUnits(); i++) {
      if (blockGroup.getParityBlocks()[i].isErased()) {
        outputBlocks[idx++] = blockGroup.getParityBlocks()[i];
      }
    }

    return outputBlocks;
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
   * Perform decoding against a block blockGroup.
   * @param blockGroup
   * @return decoding step for caller to do the real work
   */
  protected abstract ErasureCodingStep prepareDecodingStep(
      ECBlockGroup blockGroup);

  /**
   * Get the number of erased blocks in the block group.
   * @param blockGroup
   * @return number of erased blocks
   */
  protected int getNumErasedBlocks(ECBlockGroup blockGroup) {
    int num = getNumErasedBlocks(blockGroup.getParityBlocks());
    num += getNumErasedBlocks(blockGroup.getDataBlocks());
    return num;
  }

  /**
   * Find out how many blocks are erased.
   * @param inputBlocks all the input blocks
   * @return number of erased blocks
   */
  protected static int getNumErasedBlocks(ECBlock[] inputBlocks) {
    int numErased = 0;
    for (int i = 0; i < inputBlocks.length; i++) {
      if (inputBlocks[i].isErased()) {
        numErased ++;
      }
    }

    return numErased;
  }

  /**
   * Get indexes of erased blocks from inputBlocks
   * @param inputBlocks
   * @return indexes of erased blocks from inputBlocks
   */
  protected int[] getErasedIndexes(ECBlock[] inputBlocks) {
    int numErased = getNumErasedBlocks(inputBlocks);
    if (numErased == 0) {
      return new int[0];
    }

    int[] erasedIndexes = new int[numErased];
    int i = 0, j = 0;
    for (; i < inputBlocks.length && j < erasedIndexes.length; i++) {
      if (inputBlocks[i].isErased()) {
        erasedIndexes[j++] = i;
      }
    }

    return erasedIndexes;
  }

  /**
   * Get erased input blocks from inputBlocks
   * @param inputBlocks
   * @return an array of erased blocks from inputBlocks
   */
  protected ECBlock[] getErasedBlocks(ECBlock[] inputBlocks) {
    int numErased = getNumErasedBlocks(inputBlocks);
    if (numErased == 0) {
      return new ECBlock[0];
    }

    ECBlock[] erasedBlocks = new ECBlock[numErased];
    int i = 0, j = 0;
    for (; i < inputBlocks.length && j < erasedBlocks.length; i++) {
      if (inputBlocks[i].isErased()) {
        erasedBlocks[j++] = inputBlocks[i];
      }
    }

    return erasedBlocks;
  }

}
