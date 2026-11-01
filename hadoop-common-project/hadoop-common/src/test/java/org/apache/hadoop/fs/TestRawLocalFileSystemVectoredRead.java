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

package org.apache.hadoop.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.impl.TrackingByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.test.LambdaTestUtils.VoidCallable;

import static org.apache.hadoop.fs.contract.ContractTestUtils.validateVectoredReadResult;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.test.LambdaTestUtils.interceptFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Exercise raw vectored-read submission failures without relying on a particular
 * asynchronous file channel provider or thread scheduling.
 */
@Timeout(60)
@ParameterizedClass(name = "direct-{0}")
@MethodSource("params")
public class TestRawLocalFileSystemVectoredRead {

  private static final byte[] DATASET = ContractTestUtils.dataset(8192, 'a', 32);

  @TempDir
  private java.nio.file.Path tempDir;

  private final boolean direct;

  private RawLocalFileSystem fs;

  private Path path;

  public TestRawLocalFileSystemVectoredRead(boolean direct) {
    this.direct = direct;
  }

  public static List<Boolean> params() {
    return List.of(false, true);
  }

  @BeforeEach
  public void setup() throws IOException {
    java.nio.file.Path file = tempDir.resolve("data");
    Files.write(file, DATASET);
    path = new Path(file.toUri());
    fs = new RawLocalFileSystem();
    fs.initialize(RawLocalFileSystem.NAME, new Configuration());
  }

  @AfterEach
  public void teardown() throws IOException {
    fs.close();
  }

  @Test
  public void testRejectedSubmissionSettlesUnsubmittedRanges() throws Exception {
    AsynchronousFileChannel channel = mock(AsynchronousFileChannel.class);
    RejectedExecutionException failure = new RejectedExecutionException("Rejected initial read");
    doThrow(failure).when(channel).read(any(ByteBuffer.class), anyLong(), anyInt(), any());
    try (TrackingByteBufferPool tracker = newTrackingPool();
         FSDataInputStream in = openWithChannel(channel)) {
      List<FileRange> ranges = ranges();
      assertThat(intercept(RejectedExecutionException.class, () -> in.readVectored(ranges,
          size -> tracker.getBuffer(direct, size), tracker::putBuffer))).isSameAs(failure);
      for (FileRange range : ranges) {
        assertFailed(range, failure);
      }
      assertAllReleased(tracker, 1);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testRejectedSubmissionPreservesEarlierReads(boolean completeImmediately)
      throws Exception {
    verifyPartialSubmissionFailure(false, completeImmediately);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testAllocationFailurePreservesEarlierReads(boolean completeImmediately)
      throws Exception {
    verifyPartialSubmissionFailure(true, completeImmediately);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testRejectedPartialReadResubmission(boolean completeInline) throws Exception {
    AsynchronousFileChannel channel = mock(AsynchronousFileChannel.class);
    RejectedExecutionException failure = new RejectedExecutionException("Rejected remaining read");
    List<PendingRead> pending = new ArrayList<>();
    doAnswer(invocation -> {
      long position = invocation.getArgument(1);
      int index = invocation.getArgument(2);
      if (index == 0 && position != 0) {
        throw failure;
      }
      PendingRead read = new PendingRead(invocation.getArgument(0), position, index,
          invocation.getArgument(3));
      pending.add(read);
      if (completeInline && index == 0) {
        read.complete(512);
      }
      return null;
    }).when(channel).read(any(ByteBuffer.class), anyLong(), anyInt(), any());
    try (TrackingByteBufferPool tracker = newTrackingPool();
         FSDataInputStream in = openWithChannel(channel)) {
      List<FileRange> ranges = ranges().subList(0, 2);
      in.readVectored(ranges, size -> tracker.getBuffer(direct, size), tracker::putBuffer);
      // The callback must translate a rejected resubmission into a failed future,
      // rather than throwing out of the completion handler and stranding its buffer.
      if (!completeInline) {
        pending.get(0).complete(512);
      }
      assertFailed(ranges.get(0), failure);
      assertThat(tracker.getBufferReleases()).isEqualTo(1);
      assertThat(ranges.get(1).getData().isDone()).isFalse();
      assertThat(tracker.containsBuffer(pending.get(1).buffer)).isTrue();
      pending.get(1).complete(ranges.get(1).getLength());
      validateVectoredReadResult(List.of(ranges.get(1)), DATASET, 0);
      tracker.putBuffer(ranges.get(1).getData().join());
      assertAllReleased(tracker, 2);
    }
  }

  @Test
  public void testChannelOpenFailureSettlesRanges() throws Exception {
    IOException failure = new IOException("Cannot open asynchronous channel");
    try (TrackingByteBufferPool tracker = newTrackingPool();
         FSDataInputStream in = new FSDataInputStream(fs.new LocalFSFileInputStream(path) {
           @Override
           AsynchronousFileChannel getAsyncChannel() throws IOException {
             throw failure;
           }
         })) {
      List<FileRange> ranges = ranges();
      assertThat(intercept(IOException.class, () -> in.readVectored(ranges,
          size -> tracker.getBuffer(direct, size), tracker::putBuffer))).isSameAs(failure);
      for (FileRange range : ranges) {
        assertFailed(range, failure);
      }
      assertAllReleased(tracker, 0);
    }
  }

  @Test
  public void testBufferLimitFailureReleasesAllocatedBuffer() throws Exception {
    AsynchronousFileChannel channel = mock(AsynchronousFileChannel.class);
    try (TrackingByteBufferPool tracker = newTrackingPool();
         FSDataInputStream in = openWithChannel(channel)) {
      List<FileRange> ranges = ranges();
      IllegalArgumentException failure = intercept(IllegalArgumentException.class,
          () -> in.readVectored(ranges, size -> tracker.getBuffer(direct, size - 1),
              tracker::putBuffer));
      for (FileRange range : ranges) {
        assertFailed(range, failure);
      }
      assertAllReleased(tracker, 1);
      verifyNoInteractions(channel);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testReleaseFailureDoesNotStrandFutures(boolean rejectSubmission) throws Exception {
    AsynchronousFileChannel channel = mock(AsynchronousFileChannel.class);
    Exception failure = rejectSubmission
        ? new RejectedExecutionException("Rejected initial read") : new IOException("Read failed");
    IllegalStateException releaseFailure = new IllegalStateException("Release failed");
    List<PendingRead> pending = new ArrayList<>();
    doAnswer(invocation -> {
      if (rejectSubmission) {
        throw failure;
      }
      pending.add(new PendingRead(invocation.getArgument(0), invocation.getArgument(1),
          invocation.getArgument(2), invocation.getArgument(3)));
      return null;
    }).when(channel).read(any(ByteBuffer.class), anyLong(), anyInt(), any());
    try (TrackingByteBufferPool tracker = newTrackingPool();
         FSDataInputStream in = openWithChannel(channel)) {
      List<FileRange> ranges = rejectSubmission ? ranges() : ranges().subList(0, 1);
      VoidCallable read = () -> in.readVectored(ranges,
          size -> tracker.getBuffer(direct, size), buffer -> {
            tracker.putBuffer(buffer);
            throw releaseFailure;
          });
      if (rejectSubmission) {
        assertThat(intercept(RejectedExecutionException.class, read)).isSameAs(failure);
      } else {
        read.call();
        pending.get(0).handler.failed(failure, pending.get(0).index);
      }
      for (FileRange range : ranges) {
        assertFailed(range, failure);
      }
      assertThat(failure.getSuppressed()).containsExactly(releaseFailure);
      assertAllReleased(tracker, 1);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testRejectedSubmissionThroughLocalFileSystem(boolean checksummed) throws Exception {
    AsynchronousFileChannel channel = mock(AsynchronousFileChannel.class);
    RejectedExecutionException failure = new RejectedExecutionException("Rejected channel read");
    doThrow(failure).when(channel).read(any(ByteBuffer.class), anyLong(), anyInt(), any());
    RawLocalFileSystem raw = new RawLocalFileSystem() {
      @Override
      public FSDataInputStream open(Path file, int bufferSize) throws IOException {
        return new FSDataInputStream(new LocalFSFileInputStream(file) {
          @Override
          AsynchronousFileChannel getAsyncChannel() {
            return channel;
          }
        });
      }
    };
    try (LocalFileSystem local = new LocalFileSystem(raw);
         TrackingByteBufferPool tracker = newTrackingPool()) {
      local.initialize(RawLocalFileSystem.NAME, new Configuration());
      FileSystem writer = checksummed ? local : raw;
      try (FSDataOutputStream out = writer.create(path, true)) {
        out.write(DATASET);
      }
      try (FSDataInputStream in = local.open(path)) {
        List<FileRange> ranges = ranges();
        assertThat(intercept(RejectedExecutionException.class, () -> in.readVectored(ranges,
            size -> tracker.getBuffer(direct, size), tracker::putBuffer))).isSameAs(failure);
        if (!checksummed) {
          for (FileRange range : ranges) {
            assertFailed(range, failure);
          }
        }
        assertAllReleased(tracker, 1);
      }
    }
  }

  private void verifyPartialSubmissionFailure(boolean allocationFails, boolean completeImmediately)
      throws Exception {
    AsynchronousFileChannel channel = mock(AsynchronousFileChannel.class);
    RejectedExecutionException failure =
        new RejectedExecutionException("Partial submission failed");
    List<PendingRead> pending = new ArrayList<>();
    doAnswer(invocation -> {
      int index = invocation.getArgument(2);
      if (index == 1) {
        throw failure;
      }
      PendingRead read = new PendingRead(invocation.getArgument(0), invocation.getArgument(1),
          index, invocation.getArgument(3));
      pending.add(read);
      if (completeImmediately) {
        read.complete(read.buffer.remaining());
      }
      return null;
    }).when(channel).read(any(ByteBuffer.class), anyLong(), anyInt(), any());
    try (TrackingByteBufferPool tracker = newTrackingPool();
         FSDataInputStream in = openWithChannel(channel)) {
      List<FileRange> ranges = ranges();
      IntFunction<ByteBuffer> allocate = size -> {
        if (allocationFails && tracker.getBufferAllocations() == 1) {
          throw failure;
        }
        return tracker.getBuffer(direct, size);
      };
      assertThat(intercept(RejectedExecutionException.class,
          () -> in.readVectored(ranges, allocate, tracker::putBuffer))).isSameAs(failure);
      assertFailed(ranges.get(1), failure);
      assertFailed(ranges.get(2), failure);
      assertThat(ranges.get(0).getData().isDone()).isEqualTo(completeImmediately);
      assertThat(tracker.getBufferReleases()).isEqualTo(allocationFails ? 0 : 1);
      assertThat(tracker.containsBuffer(pending.get(0).buffer)).isTrue();
      if (!completeImmediately) {
        pending.get(0).complete(ranges.get(0).getLength());
      }
      validateVectoredReadResult(List.of(ranges.get(0)), DATASET, 0);
      tracker.putBuffer(ranges.get(0).getData().join());
      assertAllReleased(tracker, allocationFails ? 1 : 2);
    }
  }

  private FSDataInputStream openWithChannel(AsynchronousFileChannel channel) throws IOException {
    return new FSDataInputStream(fs.new LocalFSFileInputStream(path) {
      @Override
      AsynchronousFileChannel getAsyncChannel() {
        return channel;
      }
    });
  }

  private static List<FileRange> ranges() {
    return List.of(FileRange.createFileRange(0, 1024), FileRange.createFileRange(4096, 1024),
        FileRange.createFileRange(6144, 512));
  }

  private static TrackingByteBufferPool newTrackingPool() {
    return TrackingByteBufferPool.wrap(new ElasticByteBufferPool());
  }

  private static void assertAllReleased(TrackingByteBufferPool tracker, int expected) {
    assertThat(tracker.getBufferAllocations()).isEqualTo(expected);
    assertThat(tracker.getBufferReleases()).isEqualTo(expected);
    assertThat(tracker.size()).isZero();
  }

  private static void assertFailed(FileRange range, Exception failure) throws Exception {
    assertThat(interceptFuture(failure.getClass(), "", 5, TimeUnit.SECONDS, range.getData()))
        .isSameAs(failure);
  }

  private static final class PendingRead {
    private final ByteBuffer buffer;
    private final long position;
    private final int index;
    private final CompletionHandler<Integer, Integer> handler;

    private PendingRead(ByteBuffer buffer, long position, int index,
        CompletionHandler<Integer, Integer> handler) {
      this.buffer = buffer;
      this.position = position;
      this.index = index;
      this.handler = handler;
    }

    private void complete(int length) {
      buffer.put(DATASET, (int) position, length);
      handler.completed(length, index);
    }
  }
}
