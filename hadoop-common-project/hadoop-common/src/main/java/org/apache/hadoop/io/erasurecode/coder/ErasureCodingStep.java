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

import java.io.IOException;

/**
 * Erasure coding step that's involved in encoding/decoding of a block group.
 */
@InterfaceAudience.Private
public interface ErasureCodingStep {

  /**
   * Input blocks of readable data involved in this step, may be data blocks
   * or parity blocks.
   * @return input blocks
   */
  ECBlock[] getInputBlocks();

  /**
   * Output blocks of writable buffers involved in this step, may be data
   * blocks or parity blocks.
   * @return output blocks
   */
  ECBlock[] getOutputBlocks();

  /**
   * Perform encoding or decoding given the input chunks, and generated results
   * will be written to the output chunks.
   * @param inputChunks
   * @param outputChunks
   */
  void performCoding(ECChunk[] inputChunks, ECChunk[] outputChunks)
      throws IOException;

  /**
   * Notify erasure coder that all the chunks of input blocks are processed so
   * the coder can be able to update internal states, considering next step.
   */
  void finish();
}
