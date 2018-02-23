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
package org.apache.hadoop.hdfs.server.datanode.erasurecode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MD5Hash;

/**
 * StripedBlockChecksumReconstructor reconstruct one or more missed striped
 * block in the striped block group, the minimum number of live striped blocks
 * should be no less than data block number. Then checksum will be recalculated
 * using the newly reconstructed block.
 */
@InterfaceAudience.Private
public class StripedBlockChecksumReconstructor extends StripedReconstructor {

  private ByteBuffer targetBuffer;
  private final byte[] targetIndices;

  private byte[] checksumBuf;
  private DataOutputBuffer checksumWriter;
  private MD5Hash md5;
  private long checksumDataLen;
  private long requestedLen;

  public StripedBlockChecksumReconstructor(ErasureCodingWorker worker,
      StripedReconstructionInfo stripedReconInfo,
      DataOutputBuffer checksumWriter,
      long requestedBlockLength) throws IOException {
    super(worker, stripedReconInfo);
    this.targetIndices = stripedReconInfo.getTargetIndices();
    assert targetIndices != null;
    this.checksumWriter = checksumWriter;
    this.requestedLen = requestedBlockLength;
    init();
  }

  private void init() throws IOException {
    initDecoderIfNecessary();
    getStripedReader().init();
    // allocate buffer to keep the reconstructed block data
    targetBuffer = allocateBuffer(getBufferSize());
    long maxTargetLen = 0L;
    for (int targetIndex : targetIndices) {
      maxTargetLen = Math.max(maxTargetLen, getBlockLen(targetIndex));
    }
    setMaxTargetLength(maxTargetLen);
    int checksumSize = getChecksum().getChecksumSize();
    int bytesPerChecksum = getChecksum().getBytesPerChecksum();
    int tmpLen = checksumSize * (getBufferSize() / bytesPerChecksum);
    checksumBuf = new byte[tmpLen];
  }

  public void reconstruct() throws IOException {
    MessageDigest digester = MD5Hash.getDigester();
    long maxTargetLength = getMaxTargetLength();
    try {
      while (requestedLen > 0 && getPositionInBlock() < maxTargetLength) {
        long remaining = maxTargetLength - getPositionInBlock();
        final int toReconstructLen = (int) Math
            .min(getStripedReader().getBufferSize(), remaining);
        // step1: read from minimum source DNs required for reconstruction.
        // The returned success list is the source DNs we do real read from
        getStripedReader().readMinimumSources(toReconstructLen);

        // step2: decode to reconstruct targets
        reconstructTargets(toReconstructLen);

        // step3: calculate checksum
        checksumDataLen += checksumWithTargetOutput(targetBuffer.array(),
            toReconstructLen, digester);

        updatePositionInBlock(toReconstructLen);
        requestedLen -= toReconstructLen;
        clearBuffers();
      }

      byte[] digest = digester.digest();
      md5 = new MD5Hash(digest);
      md5.write(checksumWriter);
    } finally {
      cleanup();
    }
  }

  private long checksumWithTargetOutput(byte[] outputData, int toReconstructLen,
      MessageDigest digester) throws IOException {
    long checksumDataLength = 0;
    // Calculate partial block checksum. There are two cases.
    // case-1) length of data bytes which is fraction of bytesPerCRC
    // case-2) length of data bytes which is less than bytesPerCRC
    if (requestedLen <= toReconstructLen) {
      int remainingLen = Math.toIntExact(requestedLen);
      outputData = Arrays.copyOf(targetBuffer.array(), remainingLen);

      int partialLength = remainingLen % getChecksum().getBytesPerChecksum();

      int checksumRemaining = (remainingLen
          / getChecksum().getBytesPerChecksum())
          * getChecksum().getChecksumSize();

      int dataOffset = 0;

      // case-1) length of data bytes which is fraction of bytesPerCRC
      if (checksumRemaining > 0) {
        remainingLen = remainingLen - partialLength;
        checksumBuf = new byte[checksumRemaining];
        getChecksum().calculateChunkedSums(outputData, dataOffset,
            remainingLen, checksumBuf, 0);
        digester.update(checksumBuf, 0, checksumBuf.length);
        checksumDataLength = checksumBuf.length;
        dataOffset = remainingLen;
      }

      // case-2) length of data bytes which is less than bytesPerCRC
      if (partialLength > 0) {
        byte[] partialCrc = new byte[getChecksum().getChecksumSize()];
        getChecksum().reset();
        getChecksum().update(outputData, dataOffset, partialLength);
        getChecksum().writeValue(partialCrc, 0, true);
        digester.update(partialCrc);
        checksumDataLength += partialCrc.length;
      }

      clearBuffers();
      // calculated checksum for the requested length, return checksum length.
      return checksumDataLength;
    }
    getChecksum().calculateChunkedSums(outputData, 0,
        outputData.length, checksumBuf, 0);

    // updates digest using the checksum array of bytes
    digester.update(checksumBuf, 0, checksumBuf.length);
    return checksumBuf.length;
  }

  private void reconstructTargets(int toReconstructLen) throws IOException {
    ByteBuffer[] inputs = getStripedReader().getInputBuffers(toReconstructLen);

    ByteBuffer[] outputs = new ByteBuffer[1];
    targetBuffer.limit(toReconstructLen);
    outputs[0] = targetBuffer;
    int[] tarIndices = new int[targetIndices.length];
    for (int i = 0; i < targetIndices.length; i++) {
      tarIndices[i] = targetIndices[i];
    }
    getDecoder().decode(inputs, tarIndices, outputs);
  }

  /**
   * Clear all associated buffers.
   */
  private void clearBuffers() {
    getStripedReader().clearBuffers();
    targetBuffer.clear();
  }

  public MD5Hash getMD5() {
    return md5;
  }

  public long getChecksumDataLen() {
    return checksumDataLen;
  }
}
