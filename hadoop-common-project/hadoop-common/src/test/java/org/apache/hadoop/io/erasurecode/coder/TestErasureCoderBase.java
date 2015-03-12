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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.erasurecode.ECBlock;
import org.apache.hadoop.io.erasurecode.ECChunk;
import org.apache.hadoop.io.erasurecode.ECBlockGroup;
import org.apache.hadoop.io.erasurecode.TestCoderBase;

/**
 * Erasure coder test base with utilities.
 */
public abstract class TestErasureCoderBase extends TestCoderBase {
  protected Class<? extends ErasureEncoder> encoderClass;
  protected Class<? extends ErasureDecoder> decoderClass;

  private Configuration conf;
  protected int numChunksInBlock = 16;

  /**
   * It's just a block for this test purpose. We don't use HDFS block here
   * at all for simple.
   */
  protected static class TestBlock extends ECBlock {
    private ECChunk[] chunks;

    // For simple, just assume the block have the chunks already ready.
    // In practice we need to read/write chunks from/to the block via file IO.
    public TestBlock(ECChunk[] chunks) {
      this.chunks = chunks;
    }
  }

  /**
   * Prepare before running the case.
   * @param conf
   * @param numDataUnits
   * @param numParityUnits
   * @param erasedIndexes
   */
  protected void prepare(Configuration conf, int numDataUnits,
                         int numParityUnits, int[] erasedIndexes) {
    this.conf = conf;
    super.prepare(numDataUnits, numParityUnits, erasedIndexes);
  }

  /**
   * Generating source data, encoding, recovering and then verifying.
   * RawErasureCoder mainly uses ECChunk to pass input and output data buffers,
   * it supports two kinds of ByteBuffers, one is array backed, the other is
   * direct ByteBuffer. Have usingDirectBuffer to indicate which case to test.
   * @param usingDirectBuffer
   */
  protected void testCoding(boolean usingDirectBuffer) {
    this.usingDirectBuffer = usingDirectBuffer;

    ErasureEncoder encoder = createEncoder();

    // Generate data and encode
    ECBlockGroup blockGroup = prepareBlockGroupForEncoding();
    // Backup all the source chunks for later recovering because some coders
    // may affect the source data.
    TestBlock[] clonedDataBlocks = cloneBlocksWithData((TestBlock[])
        blockGroup.getDataBlocks());
    // Make a copy of a strip for later comparing
    TestBlock[] toEraseBlocks = copyDataBlocksToErase(clonedDataBlocks);

    ErasureCodingStep codingStep;
    try {
      codingStep = encoder.encode(blockGroup);
      performCodingStep(codingStep);
    } finally {
      encoder.release();
    }
    // Erase the copied sources
    eraseSomeDataBlocks(clonedDataBlocks);

    //Decode
    blockGroup = new ECBlockGroup(clonedDataBlocks, blockGroup.getParityBlocks());
    ErasureDecoder decoder = createDecoder();
    try {
      codingStep = decoder.decode(blockGroup);
      performCodingStep(codingStep);
    } finally {
      decoder.release();
    }
    //Compare
    compareAndVerify(toEraseBlocks, codingStep.getOutputBlocks());
  }

  /**
   * This is typically how a coding step should be performed.
   * @param codingStep
   */
  private void performCodingStep(ErasureCodingStep codingStep) {
    // Pretend that we're opening these input blocks and output blocks.
    ECBlock[] inputBlocks = codingStep.getInputBlocks();
    ECBlock[] outputBlocks = codingStep.getOutputBlocks();
    // We allocate input and output chunks accordingly.
    ECChunk[] inputChunks = new ECChunk[inputBlocks.length];
    ECChunk[] outputChunks = new ECChunk[outputBlocks.length];

    for (int i = 0; i < numChunksInBlock; ++i) {
      // Pretend that we're reading input chunks from input blocks.
      for (int j = 0; j < inputBlocks.length; ++j) {
        inputChunks[j] = ((TestBlock) inputBlocks[j]).chunks[i];
      }

      // Pretend that we allocate and will write output results to the blocks.
      for (int j = 0; j < outputBlocks.length; ++j) {
        outputChunks[j] = allocateOutputChunk();
        ((TestBlock) outputBlocks[j]).chunks[i] = outputChunks[j];
      }

      // Given the input chunks and output chunk buffers, just call it !
      codingStep.performCoding(inputChunks, outputChunks);
    }

    codingStep.finish();
  }

  /**
   * Compare and verify if recovered blocks data are the same with the erased
   * blocks data.
   * @param erasedBlocks
   * @param recoveredBlocks
   */
  protected void compareAndVerify(ECBlock[] erasedBlocks,
                                  ECBlock[] recoveredBlocks) {
    for (int i = 0; i < erasedBlocks.length; ++i) {
      compareAndVerify(((TestBlock) erasedBlocks[i]).chunks,
          ((TestBlock) recoveredBlocks[i]).chunks);
    }
  }

  /**
   * Create erasure encoder for test.
   * @return
   */
  private ErasureEncoder createEncoder() {
    ErasureEncoder encoder;
    try {
      encoder = encoderClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create encoder", e);
    }

    encoder.initialize(numDataUnits, numParityUnits, chunkSize);
    encoder.setConf(conf);
    return encoder;
  }

  /**
   * Create the erasure decoder for the test.
   * @return
   */
  private ErasureDecoder createDecoder() {
    ErasureDecoder decoder;
    try {
      decoder = decoderClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create decoder", e);
    }

    decoder.initialize(numDataUnits, numParityUnits, chunkSize);
    decoder.setConf(conf);
    return decoder;
  }

  /**
   * Prepare a block group for encoding.
   * @return
   */
  protected ECBlockGroup prepareBlockGroupForEncoding() {
    ECBlock[] dataBlocks = new TestBlock[numDataUnits];
    ECBlock[] parityBlocks = new TestBlock[numParityUnits];

    for (int i = 0; i < numDataUnits; i++) {
      dataBlocks[i] = generateDataBlock();
    }

    for (int i = 0; i < numParityUnits; i++) {
      parityBlocks[i] = allocateOutputBlock();
    }

    return new ECBlockGroup(dataBlocks, parityBlocks);
  }

  /**
   * Generate random data and return a data block.
   * @return
   */
  protected ECBlock generateDataBlock() {
    ECChunk[] chunks = new ECChunk[numChunksInBlock];

    for (int i = 0; i < numChunksInBlock; ++i) {
      chunks[i] = generateDataChunk();
    }

    return new TestBlock(chunks);
  }

  /**
   * Copy those data blocks that's to be erased for later comparing and
   * verifying.
   * @param dataBlocks
   * @return
   */
  protected TestBlock[] copyDataBlocksToErase(TestBlock[] dataBlocks) {
    TestBlock[] copiedBlocks = new TestBlock[erasedDataIndexes.length];

    for (int i = 0; i < erasedDataIndexes.length; ++i) {
      copiedBlocks[i] = cloneBlockWithData(dataBlocks[erasedDataIndexes[i]]);
    }

    return copiedBlocks;
  }

  /**
   * Allocate an output block. Note the chunk buffer will be allocated by the
   * up caller when performing the coding step.
   * @return
   */
  protected TestBlock allocateOutputBlock() {
    ECChunk[] chunks = new ECChunk[numChunksInBlock];

    return new TestBlock(chunks);
  }

  /**
   * Clone blocks with data copied along with, avoiding affecting the original
   * blocks.
   * @param blocks
   * @return
   */
  protected static TestBlock[] cloneBlocksWithData(TestBlock[] blocks) {
    TestBlock[] results = new TestBlock[blocks.length];
    for (int i = 0; i < blocks.length; ++i) {
      results[i] = cloneBlockWithData(blocks[i]);
    }

    return results;
  }

  /**
   * Clone exactly a block, avoiding affecting the original block.
   * @param block
   * @return a new block
   */
  protected static TestBlock cloneBlockWithData(TestBlock block) {
    ECChunk[] newChunks = cloneChunksWithData(block.chunks);

    return new TestBlock(newChunks);
  }

  /**
   * Erase some data blocks specified by the indexes from the data blocks.
   * @param dataBlocks
   */
  protected void eraseSomeDataBlocks(TestBlock[] dataBlocks) {
    for (int i = 0; i < erasedDataIndexes.length; ++i) {
      eraseDataFromBlock(dataBlocks, erasedDataIndexes[i]);
    }
  }

  /**
   * Erase data from a block specified by erased index.
   * @param blocks
   * @param erasedIndex
   */
  protected void eraseDataFromBlock(TestBlock[] blocks, int erasedIndex) {
    TestBlock theBlock = blocks[erasedIndex];
    eraseDataFromChunks(theBlock.chunks);
    theBlock.setErased(true);
  }
}
