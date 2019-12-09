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

import org.apache.hadoop.io.erasurecode.ECBlock;
import org.apache.hadoop.io.erasurecode.ECChunk;

import java.io.IOException;

import static org.junit.Assert.fail;


/**
 * Erasure coder test base with utilities for hitchhiker.
 */
public abstract class TestHHErasureCoderBase extends TestErasureCoderBase{
  protected int subPacketSize = 2;

  @Override
  protected void performCodingStep(ErasureCodingStep codingStep) {
    // Pretend that we're opening these input blocks and output blocks.
    ECBlock[] inputBlocks = codingStep.getInputBlocks();
    ECBlock[] outputBlocks = codingStep.getOutputBlocks();
    // We allocate input and output chunks accordingly.
    ECChunk[] inputChunks = new ECChunk[inputBlocks.length * subPacketSize];
    ECChunk[] outputChunks = new ECChunk[outputBlocks.length * subPacketSize];

    for (int i = 0; i < numChunksInBlock; i += subPacketSize) {
      // Pretend that we're reading input chunks from input blocks.
      for (int k = 0; k < subPacketSize; ++k) {
        for (int j = 0; j < inputBlocks.length; ++j) {
          inputChunks[k * inputBlocks.length + j] = ((TestBlock)
                  inputBlocks[j]).chunks[i + k];
        }

        // Pretend that we allocate and will write output results to the blocks.
        for (int j = 0; j < outputBlocks.length; ++j) {
          outputChunks[k * outputBlocks.length + j] = allocateOutputChunk();
          ((TestBlock) outputBlocks[j]).chunks[i + k] =
                  outputChunks[k * outputBlocks.length + j];
        }
      }

      // Given the input chunks and output chunk buffers, just call it !
      try {
        codingStep.performCoding(inputChunks, outputChunks);
      } catch (IOException e) {
        fail("Unexpected IOException: " + e.getMessage());
      }
    }

    codingStep.finish();
  }
}
