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
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ECBlock;
import org.apache.hadoop.io.erasurecode.ECBlockGroup;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;

/**
 * Xor erasure decoder that decodes a block group.
 *
 * It implements {@link ErasureCoder}.
 */
@InterfaceAudience.Private
public class XORErasureDecoder extends ErasureDecoder {

  public XORErasureDecoder(ErasureCoderOptions options) {
    super(options);
  }

  @Override
  protected ErasureCodingStep prepareDecodingStep(
      final ECBlockGroup blockGroup) {
    RawErasureDecoder rawDecoder = CodecUtil.createRawDecoder(getConf(),
        ErasureCodeConstants.XOR_CODEC_NAME, getOptions());

    ECBlock[] inputBlocks = getInputBlocks(blockGroup);

    return new ErasureDecodingStep(inputBlocks,
        getErasedIndexes(inputBlocks),
        getOutputBlocks(blockGroup), rawDecoder);
  }

  /**
   * Which blocks were erased ? For XOR it's simple we only allow and return one
   * erased block, either data or parity.
   * @param blockGroup
   * @return output blocks to recover
   */
  @Override
  protected ECBlock[] getOutputBlocks(ECBlockGroup blockGroup) {
    /**
     * If more than one blocks (either data or parity) erased, then it's not
     * edible to recover. We don't have the check here since it will be done
     * by upper level: ErasreCoder call can be avoid if not possible to recover
     * at all.
     */
    int erasedNum = getNumErasedBlocks(blockGroup);
    ECBlock[] outputBlocks = new ECBlock[erasedNum];

    int idx = 0;
    for (int i = 0; i < getNumParityUnits(); i++) {
      if (blockGroup.getParityBlocks()[i].isErased()) {
        outputBlocks[idx++] = blockGroup.getParityBlocks()[i];
      }
    }

    for (int i = 0; i < getNumDataUnits(); i++) {
      if (blockGroup.getDataBlocks()[i].isErased()) {
        outputBlocks[idx++] = blockGroup.getDataBlocks()[i];
      }
    }

    return outputBlocks;
  }
}
