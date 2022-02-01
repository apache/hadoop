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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import org.apache.hadoop.fs.ByteBufferPositionedReadable;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileRangeImpl;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.test.HadoopTestBase;

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
        new FileRangeImpl(100, size));
    Assertions.assertThat(buffer)
            .describedAs("Slicing on the same offset shouldn't " +
                    "create a new buffer")
            .isEqualTo(slice);

    // try slicing a range
    final int offset = 100;
    final int sliceStart = 1024;
    final int sliceLength = 16 * 1024;
    slice = VectoredReadUtils.sliceTo(buffer, offset,
        new FileRangeImpl(offset + sliceStart, sliceLength));
    // make sure they aren't the same, but use the same backing data
    Assertions.assertThat(buffer)
            .describedAs("Slicing on new offset should " +
                    "create a new buffer")
            .isNotEqualTo(slice);
    Assertions.assertThat(buffer.array())
            .describedAs("Slicing should use the same underlying " +
                    "data")
            .isEqualTo(slice.array());
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
    FileRange base = new FileRangeImpl(2000, 1000);
    CombinedFileRange mergeBase = new CombinedFileRange(2000, 3000, base);

    // test when the gap between is too big
    assertFalse("Large gap ranges shouldn't get merged", mergeBase.merge(5000, 6000,
        new FileRangeImpl(5000, 1000), 2000, 4000));
    assertEquals("Number of ranges in merged range shouldn't increase",
            1, mergeBase.getUnderlying().size());
    assertEquals("post merge offset", 2000, mergeBase.getOffset());
    assertEquals("post merge length", 1000, mergeBase.getLength());

    // test when the total size gets exceeded
    assertFalse("Large size ranges shouldn't get merged", mergeBase.merge(5000, 6000,
        new FileRangeImpl(5000, 1000), 2001, 3999));
    assertEquals("Number of ranges in merged range shouldn't increase",
            1, mergeBase.getUnderlying().size());
    assertEquals("post merge offset", 2000, mergeBase.getOffset());
    assertEquals("post merge length", 1000, mergeBase.getLength());

    // test when the merge works
    assertTrue("ranges should get merged ", mergeBase.merge(5000, 6000,
        new FileRangeImpl(5000, 1000), 2001, 4000));
    assertEquals("post merge size", 2, mergeBase.getUnderlying().size());
    assertEquals("post merge offset", 2000, mergeBase.getOffset());
    assertEquals("post merge length", 4000, mergeBase.getLength());

    // reset the mergeBase and test with a 10:1 reduction
    mergeBase = new CombinedFileRange(200, 300, base);
    assertEquals(200, mergeBase.getOffset());
    assertEquals(100, mergeBase.getLength());
    assertTrue("ranges should get merged ", mergeBase.merge(500, 600,
        new FileRangeImpl(5000, 1000), 201, 400));
    assertEquals("post merge size", 2, mergeBase.getUnderlying().size());
    assertEquals("post merge offset", 200, mergeBase.getOffset());
    assertEquals("post merge length", 400, mergeBase.getLength());
  }

  @Test
  public void testSortAndMerge() {
    List<FileRange> input = Arrays.asList(
        new FileRangeImpl(3000, 100),
        new FileRangeImpl(2100, 100),
        new FileRangeImpl(1000, 100)
        );
    assertFalse("Ranges are non disjoint", VectoredReadUtils.isOrderedDisjoint(input, 100, 800));
    List<CombinedFileRange> outputList = VectoredReadUtils.sortAndMergeRanges(
        input, 100, 1001, 2500);
    assertEquals("merged range size", 1, outputList.size());
    CombinedFileRange output = outputList.get(0);
    assertEquals("merged range underlying size", 3, output.getUnderlying().size());
    assertEquals("range[1000,3100)", output.toString());
    assertTrue("merged output ranges are disjoint",
            VectoredReadUtils.isOrderedDisjoint(outputList, 100, 800));

    // the minSeek doesn't allow the first two to merge
    assertFalse("Ranges are non disjoint", VectoredReadUtils.isOrderedDisjoint(input, 100, 1000));
    outputList = VectoredReadUtils.sortAndMergeRanges(input, 100, 1000, 2100);
    assertEquals("merged range size", 2, outputList.size());
    assertEquals("range[1000,1100)", outputList.get(0).toString());
    assertEquals("range[2100,3100)", outputList.get(1).toString());
    assertTrue("merged output ranges are disjoint",
            VectoredReadUtils.isOrderedDisjoint(outputList, 100, 1000));

    // the maxSize doesn't allow the third range to merge
    assertFalse("Ranges are non disjoint", VectoredReadUtils.isOrderedDisjoint(input, 100, 800));
    outputList = VectoredReadUtils.sortAndMergeRanges(input, 100, 1001, 2099);
    assertEquals("merged range size", 2, outputList.size());
    assertEquals("range[1000,2200)", outputList.get(0).toString());
    assertEquals("range[3000,3100)", outputList.get(1).toString());
    assertTrue("merged output ranges are disjoint",
            VectoredReadUtils.isOrderedDisjoint(outputList, 100, 800));

    // test the round up and round down (the maxSize doesn't allow any merges)
    assertFalse("Ranges are non disjoint", VectoredReadUtils.isOrderedDisjoint(input, 16, 700));
    outputList = VectoredReadUtils.sortAndMergeRanges(input, 16, 1001, 100);
    assertEquals("merged range size", 3, outputList.size());
    assertEquals("range[992,1104)", outputList.get(0).toString());
    assertEquals("range[2096,2208)", outputList.get(1).toString());
    assertEquals("range[2992,3104)", outputList.get(2).toString());
    assertTrue("merged output ranges are disjoint",
            VectoredReadUtils.isOrderedDisjoint(outputList, 16, 700));
  }

  @Test
  public void testSortAndMergeMoreCases() throws Exception {
    List<FileRange> input = Arrays.asList(
            new FileRangeImpl(3000, 110),
            new FileRangeImpl(3000, 100),
            new FileRangeImpl(2100, 100),
            new FileRangeImpl(1000, 100)
    );
    assertFalse("Ranges are non disjoint", VectoredReadUtils.isOrderedDisjoint(input, 100, 800));
    List<CombinedFileRange> outputList = VectoredReadUtils.sortAndMergeRanges(
            input, 1, 1001, 2500);
    assertEquals("merged range size", 1, outputList.size());
    CombinedFileRange output = outputList.get(0);
    assertEquals("merged range underlying size", 4, output.getUnderlying().size());
    assertEquals("range[1000,3110)", output.toString());
    assertTrue("merged output ranges are disjoint",
            VectoredReadUtils.isOrderedDisjoint(outputList, 1, 800));

    outputList = VectoredReadUtils.sortAndMergeRanges(
            input, 100, 1001, 2500);
    assertEquals("merged range size", 1, outputList.size());
    output = outputList.get(0);
    assertEquals("merged range underlying size", 4, output.getUnderlying().size());
    assertEquals("range[1000,3200)", output.toString());
    assertTrue("merged output ranges are disjoint",
            VectoredReadUtils.isOrderedDisjoint(outputList, 1, 800));

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
        VectoredReadUtils.readRangeFrom(stream, new FileRangeImpl(1000, 100),
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
        VectoredReadUtils.readRangeFrom(stream, new FileRangeImpl(1000, 100),
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
        VectoredReadUtils.readRangeFrom(stream, new FileRangeImpl(1000, 100),
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
    result =
        VectoredReadUtils.readRangeFrom(stream, new FileRangeImpl(1000, 100),
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

  static void validateBuffer(String message, ByteBuffer buffer, int start) {
    byte expected = (byte) start;
    while (buffer.remaining() > 0) {
      assertEquals(message + " remain: " + buffer.remaining(), expected++,
          buffer.get());
    }
  }

  @Test
  public void testReadVectored() throws Exception {
    List<FileRange> input = Arrays.asList(new FileRangeImpl(0, 100),
        new FileRangeImpl(100_000, 100),
        new FileRangeImpl(200_000, 100));
    Stream stream = Mockito.mock(Stream.class);
    Mockito.doAnswer(invocation -> {
      fillBuffer(invocation.getArgument(1));
      return null;
    }).when(stream).readFully(ArgumentMatchers.anyLong(),
        ArgumentMatchers.any(ByteBuffer.class));
    // should not merge the ranges
    VectoredReadUtils.readVectored(stream, input, ByteBuffer::allocate, 100, 100);
    Mockito.verify(stream, Mockito.times(3))
        .readFully(ArgumentMatchers.anyLong(), ArgumentMatchers.any(ByteBuffer.class));
    for(int b=0; b < input.size(); ++b) {
      validateBuffer("buffer " + b, input.get(b).getData().get(), 0);
    }
  }

  @Test
  public void testReadVectoredMerge() throws Exception {
    List<FileRange> input = Arrays.asList(new FileRangeImpl(2000, 100),
        new FileRangeImpl(1000, 100),
        new FileRangeImpl(0, 100));
    Stream stream = Mockito.mock(Stream.class);
    Mockito.doAnswer(invocation -> {
      fillBuffer(invocation.getArgument(1));
      return null;
    }).when(stream).readFully(ArgumentMatchers.anyLong(),
        ArgumentMatchers.any(ByteBuffer.class));
    // should merge the ranges into a single read
    VectoredReadUtils.readVectored(stream, input, ByteBuffer::allocate, 1000, 2100);
    Mockito.verify(stream, Mockito.times(1))
        .readFully(ArgumentMatchers.anyLong(), ArgumentMatchers.any(ByteBuffer.class));
    for(int b=0; b < input.size(); ++b) {
      validateBuffer("buffer " + b, input.get(b).getData().get(), (2 - b) * 1000);
    }
  }
}
