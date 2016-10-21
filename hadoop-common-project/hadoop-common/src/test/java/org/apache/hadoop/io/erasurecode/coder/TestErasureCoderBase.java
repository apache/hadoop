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
import org.apache.hadoop.io.erasurecode.ECBlockGroup;
import org.apache.hadoop.io.erasurecode.ECChunk;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.TestCoderBase;

import java.lang.reflect.Constructor;

/**
 * Erasure coder test base with utilities.
 */
public abstract class TestErasureCoderBase extends TestCoderBase {
  protected Class<? extends ErasureCoder> encoderClass;
  protected Class<? extends ErasureCoder> decoderClass;

  private ErasureCoder encoder;
  private ErasureCoder decoder;

  protected int numChunksInBlock = 16;

  /**
   * It's just a block for this test purpose. We don't use HDFS block here
   * at all for simple.
   */
  protected static class TestBlock extends ECBlock {
    protected ECChunk[] chunks;

    // For simple, just assume the block have the chunks already ready.
    // In practice we need to read/write chunks from/to the block via file IO.
    public TestBlock(ECChunk[] chunks) {
      this.chunks = chunks;
    }
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
    prepareCoders();

    /**
     * The following runs will use 3 different chunkSize for inputs and outputs,
     * to verify the same encoder/decoder can process variable width of data.
     */
    performTestCoding(baseChunkSize, true);
    performTestCoding(baseChunkSize - 17, false);
    performTestCoding(baseChunkSize + 16, true);
  }

  private void performTestCoding(int chunkSize, boolean usingSlicedBuffer) {
    setChunkSize(chunkSize);
    prepareBufferAllocator(usingSlicedBuffer);

    // Generate data and encode
    ECBlockGroup blockGroup = prepareBlockGroupForEncoding();
    // Backup all the source chunks for later recovering because some coders
    // may affect the source data.
    TestBlock[] clonedDataBlocks =
        cloneBlocksWithData((TestBlock[]) blockGroup.getDataBlocks());
    TestBlock[] parityBlocks = (TestBlock[]) blockGroup.getParityBlocks();

    ErasureCodingStep codingStep;
    codingStep = encoder.calculateCoding(blockGroup);
    performCodingStep(codingStep);
    // Erase specified sources but return copies of them for later comparing
    TestBlock[] backupBlocks = backupAndEraseBlocks(clonedDataBlocks, parityBlocks);

    // Decode
    blockGroup = new ECBlockGroup(clonedDataBlocks, blockGroup.getParityBlocks());
    codingStep = decoder.calculateCoding(blockGroup);
    performCodingStep(codingStep);

    // Compare
    compareAndVerify(backupBlocks, codingStep.getOutputBlocks());
  }

  /**
   * This is typically how a coding step should be performed.
   * @param codingStep
   */
  protected void performCodingStep(ErasureCodingStep codingStep) {
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
      compareAndVerify(((TestBlock) erasedBlocks[i]).chunks, ((TestBlock) recoveredBlocks[i]).chunks);
    }
  }

  private void prepareCoders() {
    if (encoder == null) {
      encoder = createEncoder();
    }

    if (decoder == null) {
      decoder = createDecoder();
    }
  }

  /**
   * Create the raw erasure encoder to test
   * @return
   */
  protected ErasureCoder createEncoder() {
    ErasureCoder encoder;
    try {
      ErasureCoderOptions options = new ErasureCoderOptions(
          numDataUnits, numParityUnits, allowChangeInputs, allowDump);
      Constructor<? extends ErasureCoder> constructor =
          (Constructor<? extends ErasureCoder>)
              encoderClass.getConstructor(ErasureCoderOptions.class);
      encoder = constructor.newInstance(options);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create encoder", e);
    }

    encoder.setConf(getConf());
    return encoder;
  }

  /**
   * create the raw erasure decoder to test
   * @return
   */
  protected ErasureCoder createDecoder() {
    ErasureCoder decoder;
    try {
      ErasureCoderOptions options = new ErasureCoderOptions(
          numDataUnits, numParityUnits, allowChangeInputs, allowDump);
      Constructor<? extends ErasureCoder> constructor =
          (Constructor<? extends ErasureCoder>)
              decoderClass.getConstructor(ErasureCoderOptions.class);
      decoder = constructor.newInstance(options);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create decoder", e);
    }

    decoder.setConf(getConf());
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
   * Erase blocks to test the recovering of them. Before erasure clone them
   * first so could return themselves.
   * @param dataBlocks
   * @return clone of erased dataBlocks
   */
  protected TestBlock[] backupAndEraseBlocks(TestBlock[] dataBlocks,
                                             TestBlock[] parityBlocks) {
    TestBlock[] toEraseBlocks = new TestBlock[erasedDataIndexes.length +
                                          erasedParityIndexes.length];
    int idx = 0;
    TestBlock block;

    for (int i = 0; i < erasedDataIndexes.length; i++) {
      block = dataBlocks[erasedDataIndexes[i]];
      toEraseBlocks[idx ++] = cloneBlockWithData(block);
      eraseDataFromBlock(block);
    }

    for (int i = 0; i < erasedParityIndexes.length; i++) {
      block = parityBlocks[erasedParityIndexes[i]];
      toEraseBlocks[idx ++] = cloneBlockWithData(block);
      eraseDataFromBlock(block);
    }

    return toEraseBlocks;
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
  protected TestBlock[] cloneBlocksWithData(TestBlock[] blocks) {
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
  protected TestBlock cloneBlockWithData(TestBlock block) {
    ECChunk[] newChunks = cloneChunksWithData(block.chunks);

    return new TestBlock(newChunks);
  }

  /**
   * Erase data from a block.
   */
  protected void eraseDataFromBlock(TestBlock theBlock) {
    eraseDataFromChunks(theBlock.chunks);
    theBlock.setErased(true);
  }
}
