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

import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ECBlock;
import org.apache.hadoop.io.erasurecode.ECChunk;
import org.apache.hadoop.io.erasurecode.coder.util.HHUtil;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;

/**
 * Hitchhiker-XOR Erasure decoding step, a wrapper of all the necessary
 * information to perform a decoding step involved in the whole process of
 * decoding a block group.
 */
@InterfaceAudience.Private
public class HHXORErasureDecodingStep extends HHErasureCodingStep {
  private int pbIndex;
  private int[] piggyBackIndex;
  private int[] piggyBackFullIndex;
  private int[] erasedIndexes;
  private RawErasureDecoder rsRawDecoder;
  private RawErasureEncoder xorRawEncoder;

  /**
   * The constructor with all the necessary info.
   * @param inputBlocks
   * @param erasedIndexes the indexes of erased blocks in inputBlocks array
   * @param outputBlocks
   * @param rawDecoder underlying RS decoder for hitchhiker decoding
   * @param rawEncoder underlying XOR encoder for hitchhiker decoding
   */
  public HHXORErasureDecodingStep(ECBlock[] inputBlocks, int[] erasedIndexes,
      ECBlock[] outputBlocks, RawErasureDecoder rawDecoder,
      RawErasureEncoder rawEncoder) {
    super(inputBlocks, outputBlocks);
    this.pbIndex = rawDecoder.getNumParityUnits() - 1;
    this.erasedIndexes = erasedIndexes;
    this.rsRawDecoder = rawDecoder;
    this.xorRawEncoder = rawEncoder;

    this.piggyBackIndex = HHUtil.initPiggyBackIndexWithoutPBVec(
        rawDecoder.getNumDataUnits(), rawDecoder.getNumParityUnits());
    this.piggyBackFullIndex = HHUtil.initPiggyBackFullIndexVec(
        rawDecoder.getNumDataUnits(), piggyBackIndex);
  }

  @Override
  public void performCoding(ECChunk[] inputChunks, ECChunk[] outputChunks) {
    if (erasedIndexes.length == 0) {
      return;
    }

    ByteBuffer[] inputBuffers = ECChunk.toBuffers(inputChunks);
    ByteBuffer[] outputBuffers = ECChunk.toBuffers(outputChunks);
    performCoding(inputBuffers, outputBuffers);
  }

  private void performCoding(ByteBuffer[] inputs, ByteBuffer[] outputs) {
    final int numDataUnits = rsRawDecoder.getNumDataUnits();
    final int numParityUnits = rsRawDecoder.getNumParityUnits();
    final int numTotalUnits = numDataUnits + numParityUnits;
    final int subPacketSize = getSubPacketSize();

    ByteBuffer fisrtValidInput = HHUtil.findFirstValidInput(inputs);
    final int bufSize = fisrtValidInput.remaining();

    if (inputs.length != numTotalUnits * getSubPacketSize()) {
      throw new IllegalArgumentException("Invalid inputs length");
    }

    if (outputs.length != erasedIndexes.length * getSubPacketSize()) {
      throw new IllegalArgumentException("Invalid outputs length");
    }

    // notes:inputs length = numDataUnits * subPacketizationSize
    // first numDataUnits length is first sub-stripe,
    // second numDataUnits length is second sub-stripe
    ByteBuffer[][] newIn = new ByteBuffer[subPacketSize][numTotalUnits];
    for (int i = 0; i < subPacketSize; ++i) {
      for (int j = 0; j < numTotalUnits; ++j) {
        newIn[i][j] = inputs[i * numTotalUnits + j];
      }
    }

    ByteBuffer[][] newOut = new ByteBuffer[subPacketSize][erasedIndexes.length];
    for (int i = 0; i < subPacketSize; ++i) {
      for (int j = 0; j < erasedIndexes.length; ++j) {
        newOut[i][j] = outputs[i * erasedIndexes.length + j];
      }
    }

    if (erasedIndexes.length == 1 && erasedIndexes[0] < numDataUnits) {
      // Only reconstruct one data unit missing
      doDecodeSingle(newIn, newOut, erasedIndexes[0], bufSize,
              fisrtValidInput.isDirect());
    } else {
      doDecodeMultiAndParity(newIn, newOut, erasedIndexes, bufSize);
    }
  }

  private void doDecodeSingle(ByteBuffer[][] inputs, ByteBuffer[][] outputs,
                              int erasedLocationToFix, int bufSize,
                              boolean isDirect) {
    final int numDataUnits = rsRawDecoder.getNumDataUnits();
    final int numParityUnits = rsRawDecoder.getNumParityUnits();
    final int subPacketSize = getSubPacketSize();

    int[][] inputPositions = new int[subPacketSize][inputs[0].length];
    for (int i = 0; i < subPacketSize; ++i) {
      for (int j = 0; j < inputs[i].length; ++j) {
        if (inputs[i][j] != null) {
          inputPositions[i][j] = inputs[i][j].position();
        }
      }
    }

    ByteBuffer[] tempInputs = new ByteBuffer[numDataUnits + numParityUnits];
    for (int i = 0; i < tempInputs.length; ++i) {
      tempInputs[i] = inputs[1][i];
    }

    ByteBuffer[][] tmpOutputs = new ByteBuffer[subPacketSize][numParityUnits];
    for (int i = 0; i < getSubPacketSize(); ++i) {
      for (int j = 0; j < erasedIndexes.length; ++j) {
        tmpOutputs[i][j] = outputs[i][j];
      }

      for (int m = erasedIndexes.length; m < numParityUnits; ++m) {
        tmpOutputs[i][m] = HHUtil.allocateByteBuffer(isDirect, bufSize);
      }
    }

    // First consider the second subPacket
    int[] erasedLocation = new int[numParityUnits];
    erasedLocation[0] = erasedLocationToFix;

    // assign the erased locations based on the locations not read for
    // second subPacket but from decoding
    for (int i = 1; i < numParityUnits; i++) {
      erasedLocation[i] = numDataUnits + i;
      tempInputs[numDataUnits + i] = null;
    }

    rsRawDecoder.decode(tempInputs, erasedLocation, tmpOutputs[1]);

    int piggyBackParityIndex = piggyBackFullIndex[erasedLocationToFix];
    ByteBuffer piggyBack = HHUtil.getPiggyBackForDecode(inputs, tmpOutputs,
            piggyBackParityIndex, numDataUnits, numParityUnits, pbIndex);

    // Second consider the first subPacket.
    // get the value of the piggyback associated with the erased location
    if (isDirect) {
      // decode the erased value in the first subPacket by using the piggyback
      int idxToWrite = 0;
      doDecodeByPiggyBack(inputs[0], tmpOutputs[0][idxToWrite], piggyBack,
              erasedLocationToFix);
    } else {
      ByteBuffer buffer;
      byte[][][] newInputs = new byte[getSubPacketSize()][inputs[0].length][];
      int[][] inputOffsets = new int[getSubPacketSize()][inputs[0].length];
      byte[][][] newOutputs = new byte[getSubPacketSize()][numParityUnits][];
      int[][] outOffsets = new int[getSubPacketSize()][numParityUnits];

      for (int i = 0; i < getSubPacketSize(); ++i) {
        for (int j = 0; j < inputs[0].length; ++j) {
          buffer = inputs[i][j];
          if (buffer != null) {
            inputOffsets[i][j] = buffer.arrayOffset() + buffer.position();
            newInputs[i][j] = buffer.array();
          }
        }
      }

      for (int i = 0; i < getSubPacketSize(); ++i) {
        for (int j = 0; j < numParityUnits; ++j) {
          buffer = tmpOutputs[i][j];
          if (buffer != null) {
            outOffsets[i][j] = buffer.arrayOffset() + buffer.position();
            newOutputs[i][j] = buffer.array();
          }
        }
      }

      byte[] newPiggyBack = piggyBack.array();

      // decode the erased value in the first subPacket by using the piggyback
      int idxToWrite = 0;
      doDecodeByPiggyBack(newInputs[0], inputOffsets[0],
              newOutputs[0][idxToWrite], outOffsets[0][idxToWrite],
              newPiggyBack, erasedLocationToFix, bufSize);
    }

    for (int i = 0; i < subPacketSize; ++i) {
      for (int j = 0; j < inputs[i].length; ++j) {
        if (inputs[i][j] != null) {
          inputs[i][j].position(inputPositions[i][j] + bufSize);
        }
      }
    }
  }

  private void doDecodeByPiggyBack(ByteBuffer[] inputs,
                                   ByteBuffer outputs,
                                   ByteBuffer piggyBack,
                                   int erasedLocationToFix) {
    final int thisPiggyBackSetIdx = piggyBackFullIndex[erasedLocationToFix];
    final int startIndex = piggyBackIndex[thisPiggyBackSetIdx - 1];
    final int endIndex = piggyBackIndex[thisPiggyBackSetIdx];

    // recover first sub-stripe data by XOR piggyback
    int bufSize = piggyBack.remaining();
    for (int i = piggyBack.position();
         i < piggyBack.position() + bufSize; i++) {
      for (int j = startIndex; j < endIndex; j++) {
        if (inputs[j] != null) {
          piggyBack.put(i, (byte)
                  (piggyBack.get(i) ^ inputs[j].get(inputs[j].position() + i)));
        }
      }
      outputs.put(outputs.position() + i, piggyBack.get(i));
    }
  }

  private void doDecodeByPiggyBack(byte[][] inputs, int[] inputOffsets,
                                   byte[] outputs, int outOffset,
                                   byte[] piggyBack, int erasedLocationToFix,
                                   int bufSize) {
    final int thisPiggyBackSetIdx = piggyBackFullIndex[erasedLocationToFix];
    final int startIndex = piggyBackIndex[thisPiggyBackSetIdx - 1];
    final int endIndex = piggyBackIndex[thisPiggyBackSetIdx];

    // recover first sub-stripe data by XOR piggyback
    for (int i = 0; i < bufSize; i++) {
      for (int j = startIndex; j < endIndex; j++) {
        if (inputs[j] != null) {
          piggyBack[i] = (byte) (piggyBack[i] ^ inputs[j][i + inputOffsets[j]]);
        }
      }
      outputs[i + outOffset] = piggyBack[i];
    }
  }

  private void doDecodeMultiAndParity(ByteBuffer[][] inputs,
                                      ByteBuffer[][] outputs,
                                      int[] erasedLocationToFix, int bufSize) {
    final int numDataUnits = rsRawDecoder.getNumDataUnits();
    final int numParityUnits = rsRawDecoder.getNumParityUnits();
    final int numTotalUnits = numDataUnits + numParityUnits;
    int[] parityToFixFlag = new int[numTotalUnits];

    for (int i = 0; i < erasedLocationToFix.length; ++i) {
      if (erasedLocationToFix[i] >= numDataUnits) {
        parityToFixFlag[erasedLocationToFix[i]] = 1;
      }
    }

    int[] inputPositions = new int[inputs[0].length];
    for (int i = 0; i < inputPositions.length; i++) {
      if (inputs[0][i] != null) {
        inputPositions[i] = inputs[0][i].position();
      }
    }

    // decoded first sub-stripe
    rsRawDecoder.decode(inputs[0], erasedLocationToFix, outputs[0]);

    for (int i = 0; i < inputs[0].length; i++) {
      if (inputs[0][i] != null) {
        // dataLen bytes consumed
        inputs[0][i].position(inputPositions[i]);
      }
    }

    ByteBuffer[] tempInput = new ByteBuffer[numDataUnits];
    for (int i = 0; i < numDataUnits; ++i) {
      tempInput[i] = inputs[0][i];
//
//      if (!isDirect && tempInput[i] != null) {
//        tempInput[i].position(tempInput[i].position() - bufSize);
//      }
    }

    for (int i = 0; i < erasedLocationToFix.length; ++i) {
      if (erasedLocationToFix[i] < numDataUnits) {
        tempInput[erasedLocationToFix[i]] = outputs[0][i];
      }
    }

    ByteBuffer[] piggyBack = HHUtil.getPiggyBacksFromInput(tempInput,
            piggyBackIndex, numParityUnits, 0, xorRawEncoder);

    for (int j = numDataUnits + 1; j < numTotalUnits; ++j) {
      if (parityToFixFlag[j] == 0 && inputs[1][j] != null) {
        // f(b) + f(a1,a2,a3....)
        for (int k = inputs[1][j].position(),
             m = piggyBack[j - numDataUnits - 1].position();
             k < inputs[1][j].limit(); ++k, ++m) {
          inputs[1][j].put(k, (byte)
                  (inputs[1][j].get(k) ^
                          piggyBack[j - numDataUnits - 1].get(m)));
        }
      }
    }

    // decoded second sub-stripe
    rsRawDecoder.decode(inputs[1], erasedLocationToFix, outputs[1]);

    // parity index = 0, the data have no piggyBack
    for (int j = 0; j < erasedLocationToFix.length; ++j) {
      if (erasedLocationToFix[j] < numTotalUnits
              && erasedLocationToFix[j] > numDataUnits) {
        int parityIndex = erasedLocationToFix[j] - numDataUnits - 1;
        for (int k = outputs[1][j].position(),
             m = piggyBack[parityIndex].position();
             k < outputs[1][j].limit(); ++k, ++m) {
          outputs[1][j].put(k, (byte)
                  (outputs[1][j].get(k) ^ piggyBack[parityIndex].get(m)));
        }
      }
    }

    for (int i = 0; i < inputs[0].length; i++) {
      if (inputs[0][i] != null) {
        // dataLen bytes consumed
        inputs[0][i].position(inputPositions[i] + bufSize);
      }
    }
  }

}
