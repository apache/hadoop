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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.IntFunction;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;
import org.assertj.core.api.ObjectAssert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import org.apache.hadoop.fs.ByteBufferPositionedReadable;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.VectoredReadUtils;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.test.HadoopTestBase;

import static java.util.Arrays.asList;
import static org.apache.hadoop.fs.FileRange.createFileRange;
import static org.apache.hadoop.fs.VectoredReadUtils.isOrderedDisjoint;
import static org.apache.hadoop.fs.VectoredReadUtils.mergeSortedRanges;
import static org.apache.hadoop.fs.VectoredReadUtils.readRangeFrom;
import static org.apache.hadoop.fs.VectoredReadUtils.readVectored;
import static org.apache.hadoop.fs.VectoredReadUtils.sortRangeList;
import static org.apache.hadoop.fs.VectoredReadUtils.sortRanges;
import static org.apache.hadoop.fs.VectoredReadUtils.validateAndSortRanges;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.test.MoreAsserts.assertFutureCompletedSuccessfully;
import static org.apache.hadoop.test.MoreAsserts.assertFutureFailedExceptionally;

/**
 * Test behavior of {@link VectoredReadUtils}.
 */
public class TestVectoredReadUtils extends HadoopTestBase {

  /**
   * Test {@link VectoredReadUtils#sliceTo(ByteBuffer, long, FileRange)}.
   */
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
        .describedAs("Slicing on new offset should create a new buffer")
        .isNotEqualTo(slice);
    Assertions.assertThat(buffer.array())
        .describedAs("Slicing should use the same underlying data")
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

  /**
   * Test {@link VectoredReadUtils#roundUp(long, int)}
   * and {@link VectoredReadUtils#roundDown(long, int)}.
   */
  @Test
  public void testRounding() {
    for (int i = 5; i < 10; ++i) {
      assertEquals("i = " + i, 5, VectoredReadUtils.roundDown(i, 5));
      assertEquals("i = " + i, 10, VectoredReadUtils.roundUp(i + 1, 5));
    }
    assertEquals("Error while roundDown", 13, VectoredReadUtils.roundDown(13, 1));
    assertEquals("Error while roundUp", 13, VectoredReadUtils.roundUp(13, 1));
  }

  /**
   * Test {@link CombinedFileRange#merge(long, long, FileRange, int, int)}.
   */
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
    assertFalse("Large size ranges shouldn't get merged",
        mergeBase.merge(5000, 6000,
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
  private static ListAssert<FileRange> assertUnderlyingSize(
      final CombinedFileRange combinedFileRange,
      final String description,
      final int expected) {
    return Assertions.assertThat(combinedFileRange.getUnderlying())
        .describedAs(description)
        .hasSize(expected);
  }

  /**
   * Test sort and merge logic.
   */
  @Test
  public void testSortAndMerge() {
    List<FileRange> input = asList(
        createFileRange(3000, 100, "1"),
        createFileRange(2100, 100, null),
        createFileRange(1000, 100, "3")
        );
    assertIsNotOrderedDisjoint(input, 100, 800);
    final List<CombinedFileRange> outputList = mergeSortedRanges(
            sortRangeList(input), 100, 1001, 2500);

    assertRangeListSize(outputList, 1);
    CombinedFileRange output = outputList.get(0);
    assertUnderlyingSize(output, "merged range underlying size", 3);
    // range[1000,3100)
    assertFileRange(output, 1000, 2100);
    assertOrderedDisjoint(outputList, 100, 800);

    // the minSeek doesn't allow the first two to merge
    assertIsNotOrderedDisjoint(input, 100, 100);
    final List<CombinedFileRange> list2 = mergeSortedRanges(
        sortRangeList(input),
            100, 1000, 2100);
    assertRangeListSize(list2, 2);
    assertRangeElement(list2, 0, 1000, 100);
    assertRangeElement(list2, 1, 2100, 1000);

    assertOrderedDisjoint(list2, 100, 1000);

    // the maxSize doesn't allow the third range to merge
    assertIsNotOrderedDisjoint(input, 100, 800);
    final List<CombinedFileRange> list3 = mergeSortedRanges(
        sortRangeList(input),
            100, 1001, 2099);
    assertRangeListSize(list3, 2);
    CombinedFileRange range0 = list3.get(0);
    assertFileRange(range0, 1000, 1200);
    final List<FileRange> underlying = range0.getUnderlying();
    assertFileRange(underlying.get(0),
        1000, 100, "3");
    assertFileRange(underlying.get(1),
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
        sortRangeList(input),
        16, 1001, 100);
    assertRangeListSize(list4, 3);
    // range[992,1104)
    assertRangeElement(list4, 0, 992, 112);
    // range[2096,2208)
    assertRangeElement(list4, 1, 2096, 112);
    // range[2992,3104)
    assertRangeElement(list4, 2, 2992, 112);
    assertOrderedDisjoint(list4, 16, 700);
  }

  /**
   * Assert that a file range has the specified start position and length.
   * @param range range to validate
   * @param start offset of range
   * @param length range length
   * @param <ELEMENT> type of range
   */
  private static <ELEMENT extends FileRange> void assertFileRange(
      ELEMENT range, long start, int length) {

    Assertions.assertThat(range)
        .describedAs("file range %s", range)
        .isNotNull();
    Assertions.assertThat(range.getOffset())
        .describedAs("offset of %s", range)
        .isEqualTo(start);
    Assertions.assertThat(range.getLength())
        .describedAs("length of %s", range)
        .isEqualTo(length);
  }

  /**
   * Verify that {@link VectoredReadUtils#sortRanges(List)}
   * returns an array matching the list sort ranges.
   */
  @Test
  public void testArraySortRange() throws Throwable {
    List<FileRange> input = asList(
        createFileRange(3000, 100, "1"),
        createFileRange(2100, 100, null),
        createFileRange(1000, 100, "3")
        );
    final FileRange[] rangeArray = sortRanges(input);
    final List<? extends FileRange> rangeList = sortRangeList(input);
    Assertions.assertThat(rangeArray)
        .describedAs("range array from sortRanges()")
        .isSortedAccordingTo(Comparator.comparingLong(FileRange::getOffset));
    Assertions.assertThat(rangeList.toArray(new FileRange[0]))
        .describedAs("range from sortRangeList()")
        .isEqualTo(rangeArray);
  }

  /**
   * Assert that a file range satisfies the conditions.
   * @param range range to validate
   * @param offset offset of range
   * @param length range length
   * @param reference reference; may be null.
   * @param <ELEMENT> type of range
   */
  private static <ELEMENT extends FileRange> void assertFileRange(
      ELEMENT range, long offset, int length, Object reference) {

    assertFileRange(range, offset, length);
    Assertions.assertThat(range.getReference())
        .describedAs("reference field of file range %s", range)
        .isEqualTo(reference);
  }

  /**
   * Assert that a range list has a single element with the given start and length.
   * @param ranges range list
   * @param start start position
   * @param length length of range
   * @param <ELEMENT> type of range
   * @return the ongoing assertion.
   */
  private static <ELEMENT extends FileRange> ObjectAssert<ELEMENT> assertIsSingleRange(
      final List<ELEMENT> ranges,
      final long start,
      final int length) {
    assertRangeListSize(ranges, 1);
    return assertRangeElement(ranges, 0, start, length);
  }

  /**
   * Assert that a range list has the exact size specified.
   * @param ranges range list
   * @param size expected size
   * @param <ELEMENT> type of range
   * @return the ongoing assertion.
   */
  private static <ELEMENT extends FileRange> ListAssert<ELEMENT> assertRangeListSize(
      final List<ELEMENT> ranges,
      final int size) {
    return Assertions.assertThat(ranges)
        .describedAs("coalesced ranges")
        .hasSize(size);
  }

  /**
   * Assert that a range list has at least the size specified.
   * @param ranges range list
   * @param size expected size
   * @param <ELEMENT> type of range
   * @return the ongoing assertion.
   */
  private static <ELEMENT extends FileRange> ListAssert<ELEMENT> assertRangesCountAtLeast(
      final List<ELEMENT> ranges,
      final int size) {
    return Assertions.assertThat(ranges)
        .describedAs("coalesced ranges")
        .hasSizeGreaterThanOrEqualTo(size);
  }

  /**
   * Assert that a range element has the given start offset and length.
   * @param ranges range list
   * @param index index of range
   * @param start position
   * @param length length of range
   * @param <ELEMENT> type of range
   * @return the ongoing assertion.
   */
  private static <ELEMENT extends FileRange> ObjectAssert<ELEMENT> assertRangeElement(
      final List<ELEMENT> ranges,
      final int index,
      final long start,
      final int length) {
    return assertRangesCountAtLeast(ranges, index + 1)
        .element(index)
        .describedAs("range")
        .satisfies(r -> assertFileRange(r, start, length));
  }

  /**
   * Assert that a file range is ordered and disjoint.
   * @param input the list of input ranges.
   * @param chunkSize the size of the chunks that the offset and end must align to.
   * @param minimumSeek the minimum distance between ranges.
   */
  private static void assertOrderedDisjoint(
      List<? extends FileRange> input,
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
  private static <ELEMENT extends FileRange> void assertIsNotOrderedDisjoint(
      List<ELEMENT> input,
      int chunkSize,
      int minimumSeek) {
    Assertions.assertThat(isOrderedDisjoint(input, chunkSize, minimumSeek))
        .describedAs("Ranges are non disjoint/ordered")
        .isFalse();
  }

  /**
   * Test sort and merge.
   */
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
            sortRangeList(input), 1, 1001, 2500);
    Assertions.assertThat(outputList)
            .describedAs("merged range size")
            .hasSize(1);
    CombinedFileRange output = outputList.get(0);
    assertUnderlyingSize(output, "merged range underlying size", 4);

    assertFileRange(output, 1000, 2110);

    assertOrderedDisjoint(outputList, 1, 800);

    outputList = mergeSortedRanges(
            sortRangeList(input), 100, 1001, 2500);
    assertRangeListSize(outputList, 1);

    output = outputList.get(0);
    assertUnderlyingSize(output, "merged range underlying size", 4);
    assertFileRange(output, 1000, 2200);

    assertOrderedDisjoint(outputList, 1, 800);
  }

  @Test
  public void testRejectOverlappingRanges()  throws Exception {
    List<FileRange> input = asList(
            createFileRange(100, 100),
            createFileRange(200, 100),
            createFileRange(250, 100)
    );

    intercept(IllegalArgumentException.class,
        () -> validateAndSortRanges(input, Optional.empty()));
  }

  /**
   * Special case of overlap: the ranges are equal.
   */
  @Test
  public void testDuplicateRangesRaisesIllegalArgument() throws Exception {

    List<FileRange> input1 = asList(
            createFileRange(100, 100),
            createFileRange(500, 100),
            createFileRange(1000, 100),
            createFileRange(1000, 100)
    );

    intercept(IllegalArgumentException.class,
        () -> validateAndSortRanges(input1, Optional.empty()));
  }

  /**
   * Consecutive ranges MUST pass.
   */
  @Test
  public void testConsecutiveRangesAreValid() throws Throwable {

    validateAndSortRanges(
        asList(
            createFileRange(100, 100),
            createFileRange(200, 100),
            createFileRange(300, 100)),
        Optional.empty());
  }

  /**
   * If the maximum zie for merging is zero, ranges do not get merged.
   */
  @Test
  public void testMaxSizeZeroDisablesMerging() {
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
  private static void assertEqualRangeCountsAfterMerging(List<FileRange> inputRanges,
                                                  int chunkSize,
                                                  int minimumSeek,
                                                  int maxSize) {
    List<CombinedFileRange> combinedFileRanges = mergeSortedRanges(
        inputRanges, chunkSize, minimumSeek, maxSize);
    assertRangeListSize(combinedFileRanges, inputRanges.size());
  }

  /**
   * Stream to read from.
   */
  interface Stream extends PositionedReadable, ByteBufferPositionedReadable {
    // nothing
  }

  /**
   * Fill a buffer with bytes incremented from 0.
   * @param buffer target buffer.
   */
  private static void fillBuffer(ByteBuffer buffer) {
    byte b = 0;
    while (buffer.remaining() > 0) {
      buffer.put(b++);
    }
  }

  /**
   * Read a single range, verify the future completed and validate the buffer
   * returned.
   */
  @Test
  public void testReadSingleRange() throws Exception {
    final Stream stream = mockStreamWithReadFully();
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
  }

  /**
   * Read a single range with IOE fault injection; verify the failure
   * is reported.
   */
  @Test
  public void testReadWithIOE() throws Exception {
    final Stream stream = mockStreamWithReadFully();

    Mockito.doThrow(new IOException("foo"))
        .when(stream).readFully(ArgumentMatchers.anyLong(),
                                ArgumentMatchers.any(ByteBuffer.class));
    CompletableFuture<ByteBuffer> result =
        readRangeFrom(stream, createFileRange(1000, 100), ByteBuffer::allocate);
    assertFutureFailedExceptionally(result);
  }

  /**
   * Read a range, first successfully, then with an IOE.
   * the output of the first read is validated.
   * @param allocate allocator to use
   */
  private static void runReadRangeFromPositionedReadable(IntFunction<ByteBuffer> allocate)
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
    validateBuffer("buffer", buffer, 0);


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

  /**
   * Read into an on heap buffer.
   */
  @Test
  public void testReadRangeArray() throws Exception {
    runReadRangeFromPositionedReadable(ByteBuffer::allocate);
  }

  /**
   * Read into an off-heap buffer.
   */
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

  /**
   * Validate basic read vectored works as expected.
   */
  @Test
  public void testReadVectored() throws Exception {
    List<FileRange> input = asList(createFileRange(0, 100),
        createFileRange(100_000, 100, "this"),
        createFileRange(200_000, 100, "that"));
    runAndValidateVectoredRead(input);
  }

  /**
   * Verify a read with length 0 completes with a buffer of size 0.
   */
  @Test
  public void testReadVectoredZeroBytes() throws Exception {
    List<FileRange> input = asList(createFileRange(0, 0, "1"),
        createFileRange(100_000, 100, "2"),
        createFileRange(200_000, 0, "3"));
    runAndValidateVectoredRead(input);
    // look up by name and validate.
    final FileRange r1 = retrieve(input, "1");
    Assertions.assertThat(r1.getData().get().limit())
        .describedAs("Data limit of %s", r1)
        .isEqualTo(0);
  }

  /**
   * Retrieve a range from a list of ranges by its (string) reference.
   * @param input input list
   * @param key key to look up
   * @return the range
   * @throws IllegalArgumentException if the range is not found.
   */
  private static FileRange retrieve(List<FileRange> input, String key) {
    return input.stream()
        .filter(r -> key.equals(r.getReference()))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("No range with key " + key));
  }

  /**
   * Mock run a vectored read and validate the results with the assertions.
   * <ol>
   *   <li> {@code ByteBufferPositionedReadable.readFully()} is invoked once per range.</li>
   *   <li> The buffers are filled with data</li>
   * </ol>
   * @param input input ranges
   * @throws Exception failure
   */
  private void runAndValidateVectoredRead(List<FileRange> input)
          throws Exception {
    final Stream stream = mockStreamWithReadFully();
    // should not merge the ranges
    readVectored(stream, input, ByteBuffer::allocate);
    // readFully is invoked once per range
    Mockito.verify(stream, Mockito.times(input.size()))
        .readFully(ArgumentMatchers.anyLong(), ArgumentMatchers.any(ByteBuffer.class));

    // validate each buffer
    for (int b = 0; b < input.size(); ++b) {
      validateBuffer("buffer " + b, input.get(b).getData().get(), 0);
    }
  }

  /**
   * Mock a stream with {@link Stream#readFully(long, ByteBuffer)}.
   * Filling in each byte buffer.
   * @return the stream
   * @throws IOException (side effect of the mocking;
   */
  private static Stream mockStreamWithReadFully() throws IOException {
    Stream stream = Mockito.mock(Stream.class);
    Mockito.doAnswer(invocation -> {
      fillBuffer(invocation.getArgument(1));
      return null;
    }).when(stream).readFully(ArgumentMatchers.anyLong(),
            ArgumentMatchers.any(ByteBuffer.class));
    return stream;
  }

  /**
   * Empty ranges are allowed.
   */
  @Test
  public void testEmptyRangesAllowed() throws Throwable {
    validateAndSortRanges(Collections.emptyList(), Optional.empty());
  }

  /**
   * Reject negative offsets.
   */
  @Test
  public void testNegativeOffsetRaisesEOF() throws Throwable {
    intercept(EOFException.class, () ->
        validateAndSortRanges(asList(
            createFileRange(1000, 100),
            createFileRange(-1000, 100)),
            Optional.empty()));
  }

  /**
   * Reject negative lengths.
   */
  @Test
  public void testNegativePositionRaisesIllegalArgument() throws Throwable {
    intercept(IllegalArgumentException.class, () ->
        validateAndSortRanges(asList(
            createFileRange(1000, 100),
            createFileRange(1000, -100)),
            Optional.empty()));
  }

  /**
   * A read for a whole file is valid.
   */
  @Test
  public void testReadWholeFile() throws Exception {
    final int length = 1000;

    // Read whole file as one element
    final List<? extends FileRange> ranges = validateAndSortRanges(
        asList(createFileRange(0, length)),
        Optional.of((long) length));

    assertIsSingleRange(ranges, 0, length);
  }

  /**
   * A read from start of file to past EOF is rejected.
   */
  @Test
  public void testReadPastEOFRejected() throws Exception {
    final int length = 1000;
    intercept(EOFException.class, () ->
        validateAndSortRanges(
            asList(createFileRange(0, length + 1)),
            Optional.of((long) length)));
  }

  /**
   * If the start offset is at the end of the file: an EOFException.
   */
  @Test
  public void testReadStartingPastEOFRejected() throws Exception {
    final int length = 1000;
    intercept(EOFException.class, () ->
        validateAndSortRanges(
            asList(createFileRange(length, 0)),
            Optional.of((long) length)));
  }

  /**
   * A read from just below the EOF to the end of the file is valid.
   */
  @Test
  public void testReadUpToEOF() throws Exception {
    final int length = 1000;

    final int p = length - 1;
    assertIsSingleRange(
        validateAndSortRanges(
            asList(createFileRange(p, 1)),
            Optional.of((long) length)),
        p, 1);
  }

  /**
   * A read from just below the EOF to the just past the end of the file is rejected
   * with EOFException.
   */
  @Test
  public void testReadOverEOFRejected() throws Exception {
    final long length = 1000;

    intercept(EOFException.class, () ->
        validateAndSortRanges(
            asList(createFileRange(length - 1, 2)),
            Optional.of(length)));
  }

  @Test
  public void testVectorIOBufferPool() throws Throwable {
    ElasticByteBufferPool elasticByteBufferPool = new ElasticByteBufferPool();

    // inlined lambda to assert the pool size
    Consumer<Integer> assertPoolSizeEquals = (size) -> {
      Assertions.assertThat(elasticByteBufferPool.size(false))
          .describedAs("Pool size")
          .isEqualTo(size);
    };

    // build vector pool from the buffer pool operations converted to
    // allocate and release lambda expressions
    ByteBufferPool vectorBuffers = new VectorIOBufferPool(
        r -> elasticByteBufferPool.getBuffer(false, r),
        elasticByteBufferPool::putBuffer);

    assertPoolSizeEquals.accept(0);

    final ByteBuffer b1 = vectorBuffers.getBuffer(false, 100);
    final ByteBuffer b2 = vectorBuffers.getBuffer(false, 50);

    // return the first buffer for a pool size of 1
    vectorBuffers.putBuffer(b1);
    assertPoolSizeEquals.accept(1);

    // expect the returned buffer back
    ByteBuffer b3 = vectorBuffers.getBuffer(true, 100);
    Assertions.assertThat(b3)
        .describedAs("buffer returned from a get after a previous one was returned")
        .isSameAs(b1);
    assertPoolSizeEquals.accept(0);

    // return them all
    vectorBuffers.putBuffer(b2);
    vectorBuffers.putBuffer(b3);
    assertPoolSizeEquals.accept(2);

    // release does not propagate
    vectorBuffers.release();
    assertPoolSizeEquals.accept(2);

    elasticByteBufferPool.release();
  }
}
