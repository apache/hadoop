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
package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.io.erasurecode.ECChunk;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.Assume;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test {@link DecodingValidator} under various decoders.
 */
@RunWith(Parameterized.class)
public class TestDecodingValidator extends TestRawCoderBase {

  private DecodingValidator validator;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {RSRawErasureCoderFactory.class, 6, 3, new int[]{1}, new int[]{}},
        {RSRawErasureCoderFactory.class, 6, 3, new int[]{3}, new int[]{0}},
        {RSRawErasureCoderFactory.class, 6, 3, new int[]{2, 4}, new int[]{1}},
        {NativeRSRawErasureCoderFactory.class, 6, 3, new int[]{0}, new int[]{}},
        {XORRawErasureCoderFactory.class, 10, 1, new int[]{0}, new int[]{}},
        {NativeXORRawErasureCoderFactory.class, 10, 1, new int[]{0},
            new int[]{}}
    });
  }

  public TestDecodingValidator(
      Class<? extends RawErasureCoderFactory> factoryClass, int numDataUnits,
      int numParityUnits, int[] erasedDataIndexes, int[] erasedParityIndexes) {
    this.encoderFactoryClass = factoryClass;
    this.decoderFactoryClass = factoryClass;
    this.numDataUnits = numDataUnits;
    this.numParityUnits = numParityUnits;
    this.erasedDataIndexes = erasedDataIndexes;
    this.erasedParityIndexes = erasedParityIndexes;
  }

  @BeforeEach
  public void setup() {
    if (encoderFactoryClass == NativeRSRawErasureCoderFactory.class
        || encoderFactoryClass == NativeXORRawErasureCoderFactory.class) {
      Assume.assumeTrue(ErasureCodeNative.isNativeCodeLoaded());
    }
    setAllowDump(false);
  }

  /**
   * Test if the same validator can process direct and non-direct buffers.
   */
  @Test
  public void testValidate() {
    prepare(null, numDataUnits, numParityUnits, erasedDataIndexes,
        erasedParityIndexes);
    testValidate(true);
    testValidate(false);
  }

  /**
   * Test if the same validator can process variable width of data for
   * inputs and outputs.
   */
  protected void testValidate(boolean usingDirectBuffer) {
    this.usingDirectBuffer = usingDirectBuffer;
    prepareCoders(false);
    prepareValidator(false);

    performTestValidate(baseChunkSize);
    performTestValidate(baseChunkSize - 17);
    performTestValidate(baseChunkSize + 18);
  }

  protected void prepareValidator(boolean recreate) {
    if (validator == null || recreate) {
      validator = new DecodingValidator(decoder);
    }
  }

  protected void performTestValidate(int chunkSize) {
    setChunkSize(chunkSize);
    prepareBufferAllocator(false);

    // encode
    ECChunk[] dataChunks = prepareDataChunksForEncoding();
    ECChunk[] parityChunks = prepareParityChunksForEncoding();
    ECChunk[] clonedDataChunks = cloneChunksWithData(dataChunks);
    try {
      encoder.encode(dataChunks, parityChunks);
    } catch (Exception e) {
      Assertions.fail("Should not get Exception: " + e.getMessage());
    }

    // decode
    backupAndEraseChunks(clonedDataChunks, parityChunks);
    ECChunk[] inputChunks =
        prepareInputChunksForDecoding(clonedDataChunks, parityChunks);
    markChunks(inputChunks);
    ensureOnlyLeastRequiredChunks(inputChunks);
    ECChunk[] recoveredChunks = prepareOutputChunksForDecoding();
    int[] erasedIndexes = getErasedIndexesForDecoding();
    try {
      decoder.decode(inputChunks, erasedIndexes, recoveredChunks);
    } catch (Exception e) {
      Assertions.fail("Should not get Exception: " + e.getMessage());
    }

    // validate
    restoreChunksFromMark(inputChunks);
    ECChunk[] clonedInputChunks = cloneChunksWithData(inputChunks);
    ECChunk[] clonedRecoveredChunks = cloneChunksWithData(recoveredChunks);
    int[] clonedErasedIndexes = erasedIndexes.clone();

    try {
      validator.validate(clonedInputChunks, clonedErasedIndexes,
          clonedRecoveredChunks);
    } catch (Exception e) {
      Assertions.fail("Should not get Exception: " + e.getMessage());
    }

    // Check if input buffers' positions are moved to the end
    verifyBufferPositionAtEnd(clonedInputChunks);

    // Check if validator does not change recovered chunks and erased indexes
    verifyChunksEqual(recoveredChunks, clonedRecoveredChunks);
    Assertions.assertArrayEquals(
       erasedIndexes, clonedErasedIndexes, "Erased indexes should not be changed");

    // Check if validator uses correct indexes for validation
    List<Integer> validIndexesList =
        IntStream.of(CoderUtil.getValidIndexes(inputChunks)).boxed()
            .collect(Collectors.toList());
    List<Integer> newValidIndexesList =
        IntStream.of(validator.getNewValidIndexes()).boxed()
            .collect(Collectors.toList());
    List<Integer> erasedIndexesList =
        IntStream.of(erasedIndexes).boxed().collect(Collectors.toList());
    int newErasedIndex = validator.getNewErasedIndex();
    Assertions.assertTrue(
    
       newValidIndexesList.containsAll(erasedIndexesList), "Valid indexes for validation should contain"
        + " erased indexes for decoding");
    Assertions.assertTrue(
    
       validIndexesList.contains(newErasedIndex), "An erased index for validation should be contained"
        + " in valid indexes for decoding");
    Assertions.assertFalse(
    
       newValidIndexesList.contains(newErasedIndex), "An erased index for validation should not be contained"
        + " in valid indexes for validation");
  }

  private void verifyChunksEqual(ECChunk[] chunks1, ECChunk[] chunks2) {
    boolean result = Arrays.deepEquals(toArrays(chunks1), toArrays(chunks2));
    assertTrue(result, "Recovered chunks should not be changed");
  }

  /**
   * Test if validator throws {@link InvalidDecodingException} when
   * a decoded output buffer is polluted.
   */
  @Test
  public void testValidateWithBadDecoding() throws IOException {
    prepare(null, numDataUnits, numParityUnits, erasedDataIndexes,
        erasedParityIndexes);
    this.usingDirectBuffer = true;
    prepareCoders(true);
    prepareValidator(true);
    prepareBufferAllocator(false);

    // encode
    ECChunk[] dataChunks = prepareDataChunksForEncoding();
    ECChunk[] parityChunks = prepareParityChunksForEncoding();
    ECChunk[] clonedDataChunks = cloneChunksWithData(dataChunks);
    try {
      encoder.encode(dataChunks, parityChunks);
    } catch (Exception e) {
      Assertions.fail("Should not get Exception: " + e.getMessage());
    }

    // decode
    backupAndEraseChunks(clonedDataChunks, parityChunks);
    ECChunk[] inputChunks =
        prepareInputChunksForDecoding(clonedDataChunks, parityChunks);
    markChunks(inputChunks);
    ensureOnlyLeastRequiredChunks(inputChunks);
    ECChunk[] recoveredChunks = prepareOutputChunksForDecoding();
    int[] erasedIndexes = getErasedIndexesForDecoding();
    try {
      decoder.decode(inputChunks, erasedIndexes, recoveredChunks);
    } catch (Exception e) {
      Assertions.fail("Should not get Exception: " + e.getMessage());
    }

    // validate
    restoreChunksFromMark(inputChunks);
    polluteSomeChunk(recoveredChunks);
    try {
      validator.validate(inputChunks, erasedIndexes, recoveredChunks);
      Assertions.fail("Validation should fail due to bad decoding");
    } catch (InvalidDecodingException e) {
      String expected = "Failed to validate decoding";
      GenericTestUtils.assertExceptionContains(expected, e);
    }
  }
}
