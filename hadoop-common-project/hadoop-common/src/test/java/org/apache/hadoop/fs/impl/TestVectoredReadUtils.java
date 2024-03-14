/*
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

package org.apache.hadoop.fs.impl;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import org.apache.hadoop.fs.ByteBufferPositionedReadable;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.VectoredReadUtils;
import org.apache.hadoop.test.HadoopTestBase;

import static java.util.Arrays.asList;
import static org.apache.hadoop.fs.FileRange.createFileRange;
import static org.apache.hadoop.fs.VectoredReadUtils.isOrderedDisjoint;
import static org.apache.hadoop.fs.VectoredReadUtils.mergeSortedRanges;
import static org.apache.hadoop.fs.VectoredReadUtils.readRangeFrom;
import static org.apache.hadoop.fs.VectoredReadUtils.readVectored;
import static org.apache.hadoop.fs.VectoredReadUtils.sortRanges;
import static org.apache.hadoop.fs.VectoredReadUtils.validateAndSortRanges;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.test.MoreAsserts.assertFutureCompletedSuccessfully;
import static org.apache.hadoop.test.MoreAsserts.assertFutureFailedExceptionally;

/**
 * Test behavior of {@link VectoredReadUtils}.
 */
public class TestVectoredReadUtils extends HadoopTestBase {

  @Test
  public void testSliceTo() {
    final int size = 64 * 1024;
    ByteBuffer buffer = ByteBuffer.allocate(size);
    // fill the buffer with data
    IntBuffer intBuffer = buffer.asIntBuffer();
    for(int i=0; i < size / Integer.BYTES; ++i) {
      intBuffer.put(i);
    }
    // ensure we don't make unnecessary slices
    ByteBuffer slice = VectoredReadUtils.sliceTo(buffer, 100,
        createFileRange(100, size));
    Assertions.assertThat(buffer)
            .describedAs("Slicing on the same offset shouldn't " +
                    "create a new buffer")
            .isEqualTo(slice);
    Assertions.assertThat(slice.position())
        .describedAs("Slicing should return buffers starting from position 0")
        .isEqualTo(0);

    // try slicing a range
    final int offset = 100;
    final int sliceStart = 1024;
    final int sliceLength = 16 * 1024;
    slice = VectoredReadUtils.sliceTo(buffer, offset,
        createFileRange(offset + sliceStart, sliceLength));
    // make sure they aren't the same, but use the same backing data
    Assertions.assertThat(buffer)
            .describedAs("Slicing on new offset should " +
                    "create a new buffer")
            .isNotEqualTo(slice);
    Assertions.assertThat(buffer.array())
            .describedAs("Slicing should use the same underlying " +
                    "data")
            .isEqualTo(slice.array());
    Assertions.assertThat(slice.position())
        .describedAs("Slicing should return buffers starting from position 0")
        .isEqualTo(0);
    // test the contents of the slice
    intBuffer = slice.asIntBuffer();
    for(int i=0; i < sliceLength / Integer.BYTES; ++i) {
      assertEquals("i = " + i, i + sliceStart / Integer.BYTES, intBuffer.get());
    }
  }

  @Test
  public void testRounding() {
    for(int i=5; i < 10; ++i) {
      assertEquals("i = "+ i, 5, VectoredReadUtils.roundDown(i, 5));
      assertEquals("i = "+ i, 10, VectoredReadUtils.roundUp(i+1, 5));
    }
    assertEquals("Error while roundDown", 13, VectoredReadUtils.roundDown(13, 1));
    assertEquals("Error while roundUp", 13, VectoredReadUtils.roundUp(13, 1));
  }

  @Test
  public void testMerge() {
    // a reference to use for tracking
    Object tracker1 = "one";
    Object tracker2 = "two";
    FileRange base = createFileRange(2000, 1000, tracker1);
    CombinedFileRange mergeBase = new CombinedFileRange(2000, 3000, base);

    // test when the gap between is too big
    assertFalse("Large gap ranges shouldn't get merged", mergeBase.merge(5000, 6000,
        createFileRange(5000, 1000), 2000, 4000));
    assertUnderlyingSize(mergeBase,
        "Number of ranges in merged range shouldn't increase",
        1);
    assertFileRange(mergeBase, 2000, 1000);

    // test when the total size gets exceeded
    assertFalse("Large size ranges shouldn't get merged", mergeBase.merge(5000, 6000,
        createFileRange(5000, 1000), 2001, 3999));
    assertEquals("Number of ranges in merged range shouldn't increase",
            1, mergeBase.getUnderlying().size());
    assertFileRange(mergeBase, 2000, 1000);

    // test when the merge works
    assertTrue("ranges should get merged ", mergeBase.merge(5000, 6000,
        createFileRange(5000, 1000, tracker2),
        2001, 4000));
    assertUnderlyingSize(mergeBase, "merge list after merge", 2);
    assertFileRange(mergeBase, 2000, 4000);

    Assertions.assertThat(mergeBase.getUnderlying().get(0).getReference())
        .describedAs("reference of range %s", mergeBase.getUnderlying().get(0))
        .isSameAs(tracker1);
    Assertions.assertThat(mergeBase.getUnderlying().get(1).getReference())
        .describedAs("reference of range %s", mergeBase.getUnderlying().get(1))
        .isSameAs(tracker2);

    // reset the mergeBase and test with a 10:1 reduction
    mergeBase = new CombinedFileRange(200, 300, base);
    assertFileRange(mergeBase, 200, 100);

    assertTrue("ranges should get merged ", mergeBase.merge(500, 600,
        createFileRange(5000, 1000), 201, 400));
    assertUnderlyingSize(mergeBase, "merge list after merge", 2);
    assertFileRange(mergeBase, 200, 400);
  }

  /**
   * Assert that a combined file range has a specific number of underlying ranges.
   * @param combinedFileRange file range
   * @param description text for errors
   * @param expected expected value.
   */
  private static void assertUnderlyingSize(final CombinedFileRange combinedFileRange,
      final String description,
      final int expected) {
    Assertions.assertThat(combinedFileRange.getUnderlying())
        .describedAs(description)
        .hasSize(expected);
  }

  @Test
  public void testSortAndMerge() {
    List<FileRange> input = asList(
        createFileRange(3000, 100, "1"),
        createFileRange(2100, 100, null),
        createFileRange(1000, 100, "3")
        );
    assertIsNotOrderedDisjoint(input, 100, 800);
    final List<CombinedFileRange> outputList = mergeSortedRanges(
            sortRanges(input), 100, 1001, 2500);
    Assertions.assertThat(outputList)
            .describedAs("merged range size")
            .hasSize(1);
    CombinedFileRange output = outputList.get(0);
    assertUnderlyingSize(output, "merged range underlying size", 3);
    // range[1000,3100)
    assertFileRange(output, 1000, 2100);
    assertOrderedDisjoint(outputList, 100, 800);

    // the minSeek doesn't allow the first two to merge
    assertIsNotOrderedDisjoint(input, 100, 100);
    final List<CombinedFileRange> list2 = mergeSortedRanges(
        sortRanges(input),
            100, 1000, 2100);
    Assertions.assertThat(list2)
            .describedAs("merged range size")
            .hasSize(2);
    assertFileRange(list2.get(0), 1000, 100);

    // range[2100,3100)
    assertFileRange(list2.get(1), 2100, 1000);

    assertOrderedDisjoint(list2, 100, 1000);

    // the maxSize doesn't allow the third range to merge
    assertIsNotOrderedDisjoint(input, 100, 800);
    final List<CombinedFileRange> list3 = mergeSortedRanges(
        sortRanges(input),
            100, 1001, 2099);
    Assertions.assertThat(list3)
            .describedAs("merged range size")
            .hasSize(2);
    // range[1000,2200)
    CombinedFileRange range0 = list3.get(0);
    assertFileRange(range0, 1000, 1200);
    assertFileRange(range0.getUnderlying().get(0),
        1000, 100, "3");
    assertFileRange(range0.getUnderlying().get(1),
        2100, 100, null);
    CombinedFileRange range1 = list3.get(1);
    // range[3000,3100)
    assertFileRange(range1, 3000, 100);
    assertFileRange(range1.getUnderlying().get(0),
        3000, 100, "1");

    assertOrderedDisjoint(list3, 100, 800);

    // test the round up and round down (the maxSize doesn't allow any merges)
    assertIsNotOrderedDisjoint(input, 16, 700);
    final List<CombinedFileRange> list4 = mergeSortedRanges(
        sortRanges(input),
        16, 1001, 100);
    Assertions.assertThat(list4)
            .describedAs("merged range size")
            .hasSize(3);
    // range[992,1104)
    assertFileRange(list4.get(0), 992, 112);
    // range[2096,2208)
    assertFileRange(list4.get(1), 2096, 112);
    // range[2992,3104)
    assertFileRange(list4.get(2), 2992, 112);
    assertOrderedDisjoint(list4, 16, 700);
  }

  @Test
  public void testMergeCombinedRanges() throws Throwable {
    // verify that a combined range is already merged
  }

  /**
   * Assert that a file range satisfies the conditions.
   * @param range range to validate
   * @param offset offset of range
   * @param length range length
   */
  private void assertFileRange(FileRange range, long offset, int length) {
    Assertions.assertThat(range)
        .describedAs("file range %s", range)
        .isNotNull();
    Assertions.assertThat(range.getOffset())
        .describedAs("offset of %s", range)
        .isEqualTo(offset);
    Assertions.assertThat(range.getLength())
        .describedAs("length of %s", range)
        .isEqualTo(length);
  }

  /**
   * Assert that a file range satisfies the conditions.
   * @param range range to validate
   * @param offset offset of range
   * @param length range length
   * @param reference reference; may be null.
   */
  private void assertFileRange(FileRange range, long offset, int length, Object reference) {
    assertFileRange(range, offset, length);
    Assertions.assertThat(range.getReference())
        .describedAs("reference field of file range %s", range)
        .isEqualTo(reference);
  }


  @Test
  public void testSortAndMergeMoreCases() throws Exception {
    List<FileRange> input = asList(
            createFileRange(3000, 110),
            createFileRange(3000, 100),
            createFileRange(2100, 100),
            createFileRange(1000, 100)
    );
    assertIsNotOrderedDisjoint(input, 100, 800);
    List<CombinedFileRange> outputList = mergeSortedRanges(
            sortRanges(input), 1, 1001, 2500);
    Assertions.assertThat(outputList)
            .describedAs("merged range size")
            .hasSize(1);
    CombinedFileRange output = outputList.get(0);
    assertUnderlyingSize(output, "merged range underlying size", 4);

    assertFileRange(output, 1000, 2110);

    assertOrderedDisjoint(outputList, 1, 800);

    outputList = mergeSortedRanges(
            sortRanges(input), 100, 1001, 2500);
    Assertions.assertThat(outputList)
            .describedAs("merged range size")
            .hasSize(1);
    output = outputList.get(0);
    assertUnderlyingSize(output, "merged range underlying size", 4);
    assertFileRange(output, 1000, 2200);

    assertOrderedDisjoint(outputList, 1, 800);
  }

  /**
   * Assert that a file range is ordered and disjoint.
   * @param input the list of input ranges.
   * @param chunkSize the size of the chunks that the offset and end must align to.
   * @param minimumSeek the minimum distance between ranges.
   */
  private static void assertOrderedDisjoint(List<? extends FileRange> input,
      int chunkSize,
      int minimumSeek) {
    Assertions.assertThat(isOrderedDisjoint(input, chunkSize, minimumSeek))
        .describedAs("ranges are ordered and disjoint")
        .isTrue();
  }

  /**
   * Assert that a file range is not ordered or not disjoint.
   * @param input the list of input ranges.
   * @param chunkSize the size of the chunks that the offset and end must align to.
   * @param minimumSeek the minimum distance between ranges.
   */
  private static void assertIsNotOrderedDisjoint(List<? extends FileRange> input,
      int chunkSize,
      int minimumSeek) {
    Assertions.assertThat(isOrderedDisjoint(input, chunkSize, minimumSeek))
        .describedAs("Ranges are non disjoint/ordered")
        .isFalse();
  }

  @Test
  public void testValidateOverlappingRanges()  throws Exception {
    List<FileRange> input = asList(
            createFileRange(100, 100),
            createFileRange(200, 100),
            createFileRange(250, 100)
    );

    intercept(IllegalArgumentException.class,
        () -> validateAndSortRanges(input, Optional.empty()));

    List<FileRange> input1 = asList(
            createFileRange(100, 100),
            createFileRange(500, 100),
            createFileRange(1000, 100),
            createFileRange(1000, 100)
    );

    intercept(IllegalArgumentException.class,
        () -> validateAndSortRanges(input1, Optional.empty()));

    List<FileRange> input2 = asList(
            createFileRange(100, 100),
            createFileRange(200, 100),
            createFileRange(300, 100)
    );
    // consecutive ranges MUST pass.
    validateAndSortRanges(input2, Optional.empty());
  }

  @Test
  public void testMaxSizeZeroDisablesMerging() throws Exception {
    List<FileRange> randomRanges = asList(
            createFileRange(3000, 110),
            createFileRange(3000, 100),
            createFileRange(2100, 100)
    );
    assertEqualRangeCountsAfterMerging(randomRanges, 1, 1, 0);
    assertEqualRangeCountsAfterMerging(randomRanges, 1, 0, 0);
    assertEqualRangeCountsAfterMerging(randomRanges, 1, 100, 0);
  }

  /**
   * Assert that  the range count is the same after merging.
   * @param inputRanges input ranges
   * @param chunkSize chunk size for merge
   * @param minimumSeek minimum seek for merge
   * @param maxSize max size for merge
   */
  private void assertEqualRangeCountsAfterMerging(List<FileRange> inputRanges,
                                                  int chunkSize,
                                                  int minimumSeek,
                                                  int maxSize) {
    List<CombinedFileRange> combinedFileRanges = mergeSortedRanges(
        inputRanges, chunkSize, minimumSeek, maxSize);
    Assertions.assertThat(combinedFileRanges)
            .describedAs("Mismatch in number of ranges post merging")
            .hasSize(inputRanges.size());
  }

  interface Stream extends PositionedReadable, ByteBufferPositionedReadable {
    // nothing
  }

  static void fillBuffer(ByteBuffer buffer) {
    byte b = 0;
    while (buffer.remaining() > 0) {
      buffer.put(b++);
    }
  }

  @Test
  public void testReadRangeFromByteBufferPositionedReadable() throws Exception {
    Stream stream = Mockito.mock(Stream.class);
    Mockito.doAnswer(invocation -> {
      fillBuffer(invocation.getArgument(1));
      return null;
    }).when(stream).readFully(ArgumentMatchers.anyLong(),
                              ArgumentMatchers.any(ByteBuffer.class));
    CompletableFuture<ByteBuffer> result =
        readRangeFrom(stream, createFileRange(1000, 100),
        ByteBuffer::allocate);
    assertFutureCompletedSuccessfully(result);
    ByteBuffer buffer = result.get();
    assertEquals("Size of result buffer", 100, buffer.remaining());
    byte b = 0;
    while (buffer.remaining() > 0) {
      assertEquals("remain = " + buffer.remaining(), b++, buffer.get());
    }

    // test an IOException
    Mockito.reset(stream);
    Mockito.doThrow(new IOException("foo"))
        .when(stream).readFully(ArgumentMatchers.anyLong(),
                                ArgumentMatchers.any(ByteBuffer.class));
    result =
        readRangeFrom(stream, createFileRange(1000, 100),
            ByteBuffer::allocate);
    assertFutureFailedExceptionally(result);
  }

  static void runReadRangeFromPositionedReadable(IntFunction<ByteBuffer> allocate)
          throws Exception {
    PositionedReadable stream = Mockito.mock(PositionedReadable.class);
    Mockito.doAnswer(invocation -> {
      byte b=0;
      byte[] buffer = invocation.getArgument(1);
      for(int i=0; i < buffer.length; ++i) {
        buffer[i] = b++;
      }
      return null;
    }).when(stream).readFully(ArgumentMatchers.anyLong(),
        ArgumentMatchers.any(), ArgumentMatchers.anyInt(),
        ArgumentMatchers.anyInt());
    CompletableFuture<ByteBuffer> result =
        readRangeFrom(stream, createFileRange(1000, 100),
            allocate);
    assertFutureCompletedSuccessfully(result);
    ByteBuffer buffer = result.get();
    assertEquals("Size of result buffer", 100, buffer.remaining());
    byte b = 0;
    while (buffer.remaining() > 0) {
      assertEquals("remain = " + buffer.remaining(), b++, buffer.get());
    }

    // test an IOException
    Mockito.reset(stream);
    Mockito.doThrow(new IOException("foo"))
        .when(stream).readFully(ArgumentMatchers.anyLong(),
        ArgumentMatchers.any(), ArgumentMatchers.anyInt(),
        ArgumentMatchers.anyInt());
    result = readRangeFrom(stream, createFileRange(1000, 100),
            ByteBuffer::allocate);
    assertFutureFailedExceptionally(result);
  }

  @Test
  public void testReadRangeArray() throws Exception {
    runReadRangeFromPositionedReadable(ByteBuffer::allocate);
  }

  @Test
  public void testReadRangeDirect() throws Exception {
    runReadRangeFromPositionedReadable(ByteBuffer::allocateDirect);
  }

  /**
   * Validate a buffer where the first byte value is {@code start}
   * and the subsequent bytes are from that value incremented by one, wrapping
   * at 256.
   * @param message error message.
   * @param buffer buffer
   * @param start first byte of the buffer.
   */
  private static void validateBuffer(String message, ByteBuffer buffer, int start) {
    byte expected = (byte) start;
    while (buffer.remaining() > 0) {
      assertEquals(message + " remain: " + buffer.remaining(), expected,
          buffer.get());
      // increment with wrapping.
      expected = (byte) (expected + 1);
    }
  }

  @Test
  public void testReadVectored() throws Exception {
    List<FileRange> input = asList(createFileRange(0, 100),
        createFileRange(100_000, 100, "this"),
        createFileRange(200_000, 100, "that"));
    runAndValidateVectoredRead(input);
  }

  @Test
  public void testReadVectoredZeroBytes() throws Exception {
    List<FileRange> input = asList(createFileRange(0, 0, "1"),
            createFileRange(100_000, 100, "2"),
            createFileRange(200_000, 0, "3"));
    runAndValidateVectoredRead(input);
    // look up by name and validate.
    final FileRange r1 = retrieve("1", input);
    Assertions.assertThat(r1.getData().get().limit())
            .describedAs("Data limit of %s", r1)
            .isEqualTo(0);
  }

  /**
   * Retrieve a range from a list of ranges by its (string) reference.
   * @param key key to look up
   * @param input input list
   * @return the range
   * @throws IllegalArgumentException if the range is not found.
   */
  private static FileRange retrieve(String key, List<FileRange> input) {
    return input.stream()
        .filter(r -> key.equals(r.getReference()))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("No range with key " + key));
  }

  private void runAndValidateVectoredRead(List<FileRange> input)
          throws Exception {
    Stream stream = Mockito.mock(Stream.class);
    Mockito.doAnswer(invocation -> {
      fillBuffer(invocation.getArgument(1));
      return null;
    }).when(stream).readFully(ArgumentMatchers.anyLong(),
            ArgumentMatchers.any(ByteBuffer.class));
    // should not merge the ranges
    readVectored(stream, input, ByteBuffer::allocate);
    // readFully is invoked once per range
    Mockito.verify(stream, Mockito.times(input.size()))
            .readFully(ArgumentMatchers.anyLong(), ArgumentMatchers.any(ByteBuffer.class));
    for (int b = 0; b < input.size(); ++b) {
      validateBuffer("buffer " + b, input.get(b).getData().get(), 0);
    }
  }

  /**
   * Special case of overlap: the ranges are equal.
   */
  @Test
  public void testDuplicateRangesRejected() throws Exception {
    intercept(IllegalArgumentException.class, () ->
        validateAndSortRanges(asList(
            createFileRange(1000, 100),
            createFileRange(1000, 100)),
            Optional.empty()));
  }

  @Test
  public void testEmptyRangesAreInvalid() throws Throwable {
    intercept(IllegalArgumentException.class,
        () -> validateAndSortRanges(Collections.emptyList(), Optional.empty()));
  }

  /**
   * Special case of overlap: the ranges are equal.
   */
  @Test
  public void testReadPastEOFRejected() throws Exception {
    final int length = 1000;
    final Optional<Long> olen = Optional.of((long) length);

    // ok to read whole file as one element
    validateAndSortRanges(asList(createFileRange(0, length)), olen);

    // but not to go past it
    intercept(EOFException.class, () ->
        validateAndSortRanges(asList(createFileRange(0, length + 1)), olen));

    // and if the start offset is past the end, also an error
    intercept(EOFException.class, () ->
        validateAndSortRanges(asList(createFileRange(length, 0)), olen));
    intercept(EOFException.class, () ->
        validateAndSortRanges(asList(createFileRange(length - 1, 1)), olen));

  }
}
