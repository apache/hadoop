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

/**
 * Abstract class for Hitchhiker common facilities shared by
 * {@link HHXORErasureEncodingStep}and {@link HHXORErasureDecodingStep}.
 *
 * It implements {@link ErasureCodingStep}.
 */
@InterfaceAudience.Private
public abstract class HHErasureCodingStep
        implements ErasureCodingStep {

  private ECBlock[] inputBlocks;
  private ECBlock[] outputBlocks;

  private static final int SUB_PACKET_SIZE = 2;

  /**
   * Constructor given input blocks and output blocks.
   *
   * @param inputBlocks
   * @param outputBlocks
   */
  public HHErasureCodingStep(ECBlock[] inputBlocks,
                                     ECBlock[] outputBlocks) {
    this.inputBlocks = inputBlocks;
    this.outputBlocks = outputBlocks;
  }

  protected int getSubPacketSize() {
    return SUB_PACKET_SIZE;
  }

  @Override
  public ECBlock[] getInputBlocks() {
    return inputBlocks;
  }

  @Override
  public ECBlock[] getOutputBlocks() {
    return outputBlocks;
  }

  @Override
  public void finish() {
    // TODO: Finalize encoder/decoder if necessary
  }
}
