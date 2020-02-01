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

import org.apache.hadoop.fs.ByteBufferPositionedReadable;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileRangeImpl;
import org.apache.hadoop.fs.PositionedReadable;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Test behavior of {@link AsyncReaderUtils}.
 */
public class TestAsyncReaderUtils  {

  @Test
  public void testSliceTo() {
    final int SIZE = 64 * 1024;
    ByteBuffer buffer = ByteBuffer.allocate(SIZE);
    // fill the buffer with data
    IntBuffer intBuffer = buffer.asIntBuffer();
    for(int i=0; i < SIZE / Integer.BYTES; ++i) {
      intBuffer.put(i);
    }
    // ensure we don't make unnecessary slices
    ByteBuffer slice = AsyncReaderUtils.sliceTo(buffer, 100,
        new FileRangeImpl(100, SIZE));
    assertSame(buffer, slice);

    // try slicing a range
    final int OFFSET = 100;
    final int SLICE_START = 1024;
    final int SLICE_LENGTH = 16 * 1024;
    slice = AsyncReaderUtils.sliceTo(buffer, OFFSET,
        new FileRangeImpl(OFFSET + SLICE_START, SLICE_LENGTH));
    // make sure they aren't the same, but use the same backing data
    assertNotSame(buffer, slice);
    assertSame(buffer.array(), slice.array());
    // test the contents of the slice
    intBuffer = slice.asIntBuffer();
    for(int i=0; i < SLICE_LENGTH / Integer.BYTES; ++i) {
      assertEquals("i = " + i, i + SLICE_START / Integer.BYTES, intBuffer.get());
    }
  }

  @Test
  public void testRounding() {
    for(int i=5; i < 10; ++i) {
      assertEquals("i = "+ i, 5, AsyncReaderUtils.roundDown(i, 5));
      assertEquals("i = "+ i, 10, AsyncReaderUtils.roundUp(i+1, 5));
    }
    assertEquals(13, AsyncReaderUtils.roundDown(13, 1));
    assertEquals(13, AsyncReaderUtils.roundUp(13, 1));
  }

  @Test
  public void testMerge() {
    FileRange base = new FileRangeImpl(2000, 1000);
    CombinedFileRange mergeBase = new CombinedFileRange(2000, 3000, base);

    // test when the gap between is too big
    assertFalse(mergeBase.merge(5000, 6000,
        new FileRangeImpl(5000, 1000), 2000, 4000));
    assertEquals(1, mergeBase.getUnderlying().size());
    assertEquals(2000, mergeBase.getOffset());
    assertEquals(1000, mergeBase.getLength());

    // test when the total size gets exceeded
    assertFalse(mergeBase.merge(5000, 6000,
        new FileRangeImpl(5000, 1000), 2001, 3999));
    assertEquals(1, mergeBase.getUnderlying().size());
    assertEquals(2000, mergeBase.getOffset());
    assertEquals(1000, mergeBase.getLength());

    // test when the merge works
    assertTrue(mergeBase.merge(5000, 6000,
        new FileRangeImpl(5000, 1000), 2001, 4000));
    assertEquals(2, mergeBase.getUnderlying().size());
    assertEquals(2000, mergeBase.getOffset());
    assertEquals(4000, mergeBase.getLength());

    // reset the mergeBase and test with a 10:1 reduction
    mergeBase = new CombinedFileRange(200, 300, base);
    assertEquals(200, mergeBase.getOffset());
    assertEquals(100, mergeBase.getLength());
    assertTrue(mergeBase.merge(500, 600,
        new FileRangeImpl(5000, 1000), 201, 400));
    assertEquals(2, mergeBase.getUnderlying().size());
    assertEquals(200, mergeBase.getOffset());
    assertEquals(400, mergeBase.getLength());
  }

  @Test
  public void testSortAndMerge() {
    List<FileRange> input = Arrays.asList(
        new FileRangeImpl(3000, 100),
        new FileRangeImpl(2100, 100),
        new FileRangeImpl(1000, 100)
        );
    assertFalse(AsyncReaderUtils.isOrderedDisjoint(input, 100, 800));
    List<CombinedFileRange> outputList = AsyncReaderUtils.sortAndMergeRanges(
        input, 100, 1001, 2500);
    assertEquals(1, outputList.size());
    CombinedFileRange output = outputList.get(0);
    assertEquals(3, output.getUnderlying().size());
    assertEquals("range[1000,3100)", output.toString());
    assertTrue(AsyncReaderUtils.isOrderedDisjoint(outputList, 100, 800));

    // the minSeek doesn't allow the first two to merge
    assertFalse(AsyncReaderUtils.isOrderedDisjoint(input, 100, 1000));
    outputList = AsyncReaderUtils.sortAndMergeRanges(input, 100, 1000, 2100);
    assertEquals(2, outputList.size());
    assertEquals("range[1000,1100)", outputList.get(0).toString());
    assertEquals("range[2100,3100)", outputList.get(1).toString());
    assertTrue(AsyncReaderUtils.isOrderedDisjoint(outputList, 100, 1000));

    // the maxSize doesn't allow the third range to merge
    assertFalse(AsyncReaderUtils.isOrderedDisjoint(input, 100, 800));
    outputList = AsyncReaderUtils.sortAndMergeRanges(input, 100, 1001, 2099);
    assertEquals(2, outputList.size());
    assertEquals("range[1000,2200)", outputList.get(0).toString());
    assertEquals("range[3000,3100)", outputList.get(1).toString());
    assertTrue(AsyncReaderUtils.isOrderedDisjoint(outputList, 100, 800));

    // test the round up and round down (the maxSize doesn't allow any merges)
    assertFalse(AsyncReaderUtils.isOrderedDisjoint(input, 16, 700));
    outputList = AsyncReaderUtils.sortAndMergeRanges(input, 16, 1001, 100);
    assertEquals(3, outputList.size());
    assertEquals("range[992,1104)", outputList.get(0).toString());
    assertEquals("range[2096,2208)", outputList.get(1).toString());
    assertEquals("range[2992,3104)", outputList.get(2).toString());
    assertTrue(AsyncReaderUtils.isOrderedDisjoint(outputList, 16, 700));
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
        AsyncReaderUtils.readRangeFrom(stream, new FileRangeImpl(1000, 100),
        ByteBuffer::allocate);
    assertTrue(result.isDone());
    ByteBuffer buffer = result.get();
    assertEquals(100, buffer.remaining());
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
        AsyncReaderUtils.readRangeFrom(stream, new FileRangeImpl(1000, 100),
            ByteBuffer::allocate);
    assertTrue(result.isCompletedExceptionally());
  }

  static void runReadRangeFromPositionedReadable(IntFunction<ByteBuffer> allocate) throws Exception {
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
        AsyncReaderUtils.readRangeFrom(stream, new FileRangeImpl(1000, 100),
            allocate);
    assertTrue(result.isDone());
    assertFalse(result.isCompletedExceptionally());
    ByteBuffer buffer = result.get();
    assertEquals(100, buffer.remaining());
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
        AsyncReaderUtils.readRangeFrom(stream, new FileRangeImpl(1000, 100),
            ByteBuffer::allocate);
    assertTrue(result.isCompletedExceptionally());
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
  public void testReadAsync() throws Exception {
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
    AsyncReaderUtils.readAsync(stream, input, ByteBuffer::allocate, 100, 100);
    Mockito.verify(stream, Mockito.times(3))
        .readFully(ArgumentMatchers.anyLong(), ArgumentMatchers.any(ByteBuffer.class));
    for(int b=0; b < input.size(); ++b) {
      validateBuffer("buffer " + b, input.get(b).getData().get(), 0);
    }
  }

  @Test
  public void testReadAsyncMerge() throws Exception {
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
    AsyncReaderUtils.readAsync(stream, input, ByteBuffer::allocate, 1000, 2100);
    Mockito.verify(stream, Mockito.times(1))
        .readFully(ArgumentMatchers.anyLong(), ArgumentMatchers.any(ByteBuffer.class));
    for(int b=0; b < input.size(); ++b) {
      validateBuffer("buffer " + b, input.get(b).getData().get(), (2 - b) * 1000);
    }
  }
}
