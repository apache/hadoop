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
import org.apache.hadoop.io.erasurecode.ECChunk;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;

/**
 * Erasure encoding step, a wrapper of all the necessary information to perform
 * an encoding step involved in the whole process of encoding a block group.
 */
@InterfaceAudience.Private
public class ErasureEncodingStep implements ErasureCodingStep {
  private ECBlock[] inputBlocks;
  private ECBlock[] outputBlocks;
  private RawErasureEncoder rawEncoder;

  /**
   * The constructor with all the necessary info.
   * @param inputBlocks
   * @param outputBlocks
   * @param rawEncoder
   */
  public ErasureEncodingStep(ECBlock[] inputBlocks, ECBlock[] outputBlocks,
                             RawErasureEncoder rawEncoder) {
    this.inputBlocks = inputBlocks;
    this.outputBlocks = outputBlocks;
    this.rawEncoder = rawEncoder;
  }

  @Override
  public void performCoding(ECChunk[] inputChunks, ECChunk[] outputChunks) {
    rawEncoder.encode(inputChunks, outputChunks);
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
    rawEncoder.release();
  }
}
