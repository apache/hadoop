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
package org.apache.hadoop.hdfs;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.StripingChunk;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.AlignedStripe;
import org.apache.hadoop.io.erasurecode.ECChunk;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.hdfs.DFSUtilClient.CorruptedBlocks;

import java.nio.ByteBuffer;

/**
 * The reader for reading a complete {@link StripedBlockUtil.AlignedStripe}
 * which may cross multiple stripes with cellSize width.
 */
class PositionStripeReader extends StripeReader {
  private ByteBuffer codingBuffer;

  PositionStripeReader(AlignedStripe alignedStripe,
      ErasureCodingPolicy ecPolicy, LocatedBlock[] targetBlocks,
      BlockReaderInfo[] readerInfos, CorruptedBlocks corruptedBlocks,
      RawErasureDecoder decoder, DFSStripedInputStream dfsStripedInputStream) {
    super(alignedStripe, ecPolicy, targetBlocks, readerInfos,
        corruptedBlocks, decoder, dfsStripedInputStream);
  }

  @Override
  void prepareDecodeInputs() {
    if (codingBuffer == null) {
      this.decodeInputs = new ECChunk[dataBlkNum + parityBlkNum];
      initDecodeInputs(alignedStripe);
    }
  }

  @Override
  boolean prepareParityChunk(int index) {
    Preconditions.checkState(index >= dataBlkNum &&
        alignedStripe.chunks[index] == null);

    int bufLen = (int) alignedStripe.getSpanInBlock();
    decodeInputs[index] = new ECChunk(codingBuffer.duplicate(), index * bufLen,
        bufLen);

    alignedStripe.chunks[index] =
        new StripingChunk(decodeInputs[index].getBuffer());

    return true;
  }

  @Override
  void decode() {
    finalizeDecodeInputs();
    decodeAndFillBuffer(true);
  }

  void initDecodeInputs(AlignedStripe alignedStripe) {
    int bufLen = (int) alignedStripe.getSpanInBlock();
    int bufCount = dataBlkNum + parityBlkNum;
    codingBuffer = dfsStripedInputStream.getBufferPool().
        getBuffer(useDirectBuffer(), bufLen * bufCount);
    ByteBuffer buffer;
    for (int i = 0; i < dataBlkNum; i++) {
      buffer = codingBuffer.duplicate();
      decodeInputs[i] = new ECChunk(buffer, i * bufLen, bufLen);
    }

    for (int i = 0; i < dataBlkNum; i++) {
      if (alignedStripe.chunks[i] == null) {
        alignedStripe.chunks[i] =
            new StripingChunk(decodeInputs[i].getBuffer());
      }
    }
  }

  void close() {
    if (decodeInputs != null) {
      for (int i = 0; i < decodeInputs.length; i++) {
        decodeInputs[i] = null;
      }
    }

    if (codingBuffer != null) {
      dfsStripedInputStream.getBufferPool().putBuffer(codingBuffer);
      codingBuffer = null;
    }
  }
}
