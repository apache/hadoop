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

package org.apache.hadoop.fs.azurebfs.services;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.impl.CombinedFileRange;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_VECTORED_READ_STRATEGY;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.contract.ContractTestUtils.validateVectoredReadResult;

public class ITestVectoredRead extends AbstractAbfsIntegrationTest {
  private static final int FILE_1_MB = ONE_MB;
  private static final int FILE_4_MB = 4 * ONE_MB;
  private static final int FILE_8_MB = 8 * ONE_MB;
  private static final int FILE_16_MB = 16 * ONE_MB;
  private static final int FILE_32_MB = 32 * ONE_MB;
  private static final int FILE_100_MB = 100 * ONE_MB;

  private static final int OFFSET_100_B = 100;
  private static final int OFFSET_15K_B = 15_000;
  private static final int OFFSET_42K_B = 42_500;

  private static final int LEN_10K_B = 10_000;
  private static final int LEN_27K_B = 27_000;
  private static final int LEN_40K_B = 40_000;

  private static final double MB_1_2 = 1.2;
  private static final double MB_3_1 = 3.1;
  private static final double MB_4_1 = 4.1;
  private static final double MB_6_2 = 6.2;

  private static final double MB_0_8 = 0.8;
  private static final double MB_0_9 = 0.9;
  private static final double MB_1_8 = 1.8;
  private static final double MB_1_9 = 1.9;

  private static final int HUGE_OFFSET_1 = 5_856_368;
  private static final int HUGE_OFFSET_2 = 3_520_861;
  private static final int HUGE_OFFSET_3 = 8_191_913;
  private static final int HUGE_OFFSET_4 = 1_520_861;
  private static final int HUGE_OFFSET_5 = 2_520_861;
  private static final int HUGE_OFFSET_6 = 9_191_913;
  private static final int HUGE_OFFSET_7 = 2_820_861;

  private static final int HUGE_RANGE = 116_770;
  private static final int HUGE_RANGE_LARGE = 156_770;

  private static final int LOOKUP_RETRIES = 100;
  private static final int EXEC_THREADS = 3;
  private static final int SEQ_READ_ITERATIONS = 5;
  private static final int FUTURE_TIMEOUT_SEC = 5;

  public ITestVectoredRead() throws Exception {
  }

  /**
   * Verifies basic vectored read functionality with two disjoint ranges.
   * Ensures data read via readVectored matches data read via a full sequential read.
   * Acts as a correctness baseline for simple non-overlapping ranges.
   */
  @Test
  public void testDisjointRangesWithVectoredRead() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    byte[] fileContent = getRandomBytesArray(FILE_1_MB);
    Path path = createFileWithContent(fs, methodName.getMethodName(), fileContent);

    List<FileRange> ranges = new ArrayList<>();
    ranges.add(FileRange.createFileRange(OFFSET_100_B, LEN_10K_B));
    ranges.add(FileRange.createFileRange(OFFSET_15K_B, LEN_27K_B));

    try (FSDataInputStream in = fs.openFile(path).build().get()) {
      in.readVectored(ranges, ByteBuffer::allocate);
      byte[] full = new byte[FILE_1_MB];
      in.readFully(0, full);
      validateVectoredReadResult(ranges, full, 0);
    }
  }

  /**
   * Validates that disjoint vectored ranges are coalesced into the minimum
   * number of backend reads.
   * Ensures that only two remote reads are issued for the given range layout.
   */
  @Test
  public void testVectoredReadDisjointRangesExpectTwoBackendReads()
      throws Exception {

    final AzureBlobFileSystem fs = getFileSystem();
    byte[] content = getRandomBytesArray(FILE_16_MB);
    Path path = createFileWithContent(fs, methodName.getMethodName(), content);

    List<FileRange> ranges = new ArrayList<>();
    ranges.add(FileRange.createFileRange(0L, ONE_MB));
    ranges.add(FileRange.createFileRange((long) (MB_1_2 * ONE_MB),
        (int) (MB_0_8 * ONE_MB)));
    ranges.add(FileRange.createFileRange((long) (MB_3_1 * ONE_MB),
        (int) (MB_0_9 * ONE_MB)));
    ranges.add(FileRange.createFileRange((long) (MB_4_1 * ONE_MB),
        (int) (MB_1_9 * ONE_MB)));
    ranges.add(FileRange.createFileRange((long) (MB_6_2 * ONE_MB),
        (int) (MB_1_8 * ONE_MB)));

    try (FSDataInputStream in = fs.openFile(path).build().get()) {
      AbfsInputStream spy =
          Mockito.spy((AbfsInputStream) in.getWrappedStream());

      spy.readVectored(ranges, ByteBuffer::allocate);

      CompletableFuture.allOf(
              ranges.stream()
                  .map(FileRange::getData)
                  .toArray(CompletableFuture[]::new))
          .get();

      validateVectoredReadResult(ranges, content, 0);

      Mockito.verify(spy, Mockito.times(2))
          .readRemote(
              Mockito.anyLong(),
              Mockito.any(byte[].class),
              Mockito.anyInt(),
              Mockito.anyInt(),
              Mockito.any());
    }
  }

  /**
   * Simulates failure in vectored read queuing.
   * Verifies that the system safely falls back to direct reads
   * and still returns correct data.
   */
  @Test
  public void testVectoredReadFallsBackToDirectReadWhenQueuingFails()
      throws Exception {

    final AzureBlobFileSystem fs = getFileSystem();
    byte[] content = getRandomBytesArray(FILE_4_MB);
    Path path = createFileWithContent(fs, methodName.getMethodName(), content);

    List<FileRange> ranges = List.of(
        FileRange.createFileRange(0, ONE_MB),
        FileRange.createFileRange(2 * ONE_MB, ONE_MB));

    try (FSDataInputStream in = fs.openFile(path).build().get()) {
      AbfsInputStream abfsIn =
          (AbfsInputStream) in.getWrappedStream();
      AbfsInputStream spyIn = Mockito.spy(abfsIn);

      VectoredReadHandler realHandler = abfsIn.getVectoredReadHandler();
      VectoredReadHandler spyHandler = Mockito.spy(realHandler);

      Mockito.doReturn(spyHandler).when(spyIn).getVectoredReadHandler();
      Mockito.doReturn(false)
          .when(spyHandler)
          .queueVectoredRead(
              Mockito.any(),
              Mockito.any(CombinedFileRange.class),
              ArgumentMatchers.any());

      spyIn.readVectored(ranges, ByteBuffer::allocate);

      CompletableFuture.allOf(
              ranges.stream()
                  .map(FileRange::getData)
                  .toArray(CompletableFuture[]::new))
          .get();

      Mockito.verify(spyHandler, Mockito.atLeastOnce())
          .directRead(
              Mockito.any(),
              Mockito.any(CombinedFileRange.class),
              Mockito.any());

      validateVectoredReadResult(ranges, content, 0);
    }
  }

  /**
   * Tests vectored reads with multiple small disjoint ranges.
   * Ensures correctness when several non-contiguous ranges are requested together.
   */
  @Test
  public void testMultipleDisjointRangesWithVectoredRead() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    byte[] content = getRandomBytesArray(FILE_1_MB);
    Path path = createFileWithContent(fs, methodName.getMethodName(), content);

    List<FileRange> ranges = List.of(
        FileRange.createFileRange(OFFSET_100_B, LEN_10K_B),
        FileRange.createFileRange(OFFSET_15K_B, LEN_27K_B),
        FileRange.createFileRange(OFFSET_42K_B, LEN_40K_B));

    try (FSDataInputStream in = fs.openFile(path).build().get()) {
      in.readVectored(ranges, ByteBuffer::allocate);
      byte[] full = new byte[FILE_1_MB];
      in.readFully(0, full);
      validateVectoredReadResult(ranges, full, 0);
    }
  }

  /**
   * Validates vectored reads against a very large file with widely scattered ranges.
   * Ensures correctness and stability under large-file and non-localized access patterns.
   */
  @Test
  public void testVectoredIOHugeFile() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    byte[] content = getRandomBytesArray(FILE_100_MB);
    Path path = createFileWithContent(fs, methodName.getMethodName(), content);

    List<FileRange> ranges = List.of(
        FileRange.createFileRange(HUGE_OFFSET_1, HUGE_RANGE),
        FileRange.createFileRange(HUGE_OFFSET_2, HUGE_RANGE),
        FileRange.createFileRange(HUGE_OFFSET_3, HUGE_RANGE),
        FileRange.createFileRange(HUGE_OFFSET_4, HUGE_RANGE),
        FileRange.createFileRange(HUGE_OFFSET_5, HUGE_RANGE),
        FileRange.createFileRange(HUGE_OFFSET_6, HUGE_RANGE),
        FileRange.createFileRange(HUGE_OFFSET_7, HUGE_RANGE_LARGE));

    try (FSDataInputStream in = fs.openFile(path).build().get()) {
      in.readVectored(ranges, ByteBuffer::allocate);
      byte[] full = new byte[FILE_100_MB];
      in.readFully(0, full);
      validateVectoredReadResult(ranges, full, 0);
    }
  }

  /**
   * Ensures vectored reads on one file do not interfere with
   * sequential reads and readahead on a different file.
   * Validates isolation across concurrent streams.
   */
  @Test
  public void testConcurrentStreamsOnDifferentFiles() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();

    byte[] content1 = getRandomBytesArray(FILE_16_MB);
    byte[] content2 = getRandomBytesArray(FILE_16_MB);

    Path path1 = createFileWithContent(fs, "file1", content1);
    Path path2 = createFileWithContent(fs, "file2", content2);

    try (FSDataInputStream in1 = fs.openFile(path1).build().get();
         FSDataInputStream in2 = fs.openFile(path2).build().get()) {

      AbfsInputStream vStream =
          (AbfsInputStream) in1.getWrappedStream();
      AbfsInputStream sStream =
          (AbfsInputStream) in2.getWrappedStream();

      List<FileRange> ranges = List.of(
          FileRange.createFileRange(2 * ONE_MB, ONE_MB),
          FileRange.createFileRange(4 * ONE_MB, ONE_MB));

      CountDownLatch latch = new CountDownLatch(1);

      CompletableFuture<Void> vectoredTask = CompletableFuture.runAsync(() -> {
        try {
          latch.await();
          vStream.readVectored(ranges, ByteBuffer::allocate);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      CompletableFuture<Void> sequentialTask = CompletableFuture.runAsync(() -> {
        try {
          latch.await();
          for (int i = 0; i < SEQ_READ_ITERATIONS; i++) {
            byte[] buf = new byte[ONE_MB];
            sStream.read(i * ONE_MB, buf, 0, ONE_MB);
            assertArrayEquals(
                Arrays.copyOfRange(content2,
                    i * ONE_MB, (i + 1) * ONE_MB),
                buf);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      latch.countDown();
      CompletableFuture.allOf(vectoredTask, sequentialTask).get();

      CompletableFuture.allOf(
          ranges.stream().map(FileRange::getData)
              .toArray(CompletableFuture[]::new)).get();

      validateVectoredReadResult(ranges, content1, 0);
    }
  }

  /**
   * Ensures multiple reads issued while a buffer is in progress
   * properly wait and complete once the buffer finishes.
   * Covers both sequential and vectored reads during in-flight I/O.
   */
  @Test
  public void testMultipleReadsWhileBufferInProgressEventuallyComplete()
      throws Exception {

    final AzureBlobFileSystem fs = getFileSystem();
    byte[] content = getRandomBytesArray(FILE_8_MB);
    Path path = createFileWithContent(fs, methodName.getMethodName(), content);

    CountDownLatch block = new CountDownLatch(1);

    try (FSDataInputStream in = fs.openFile(path).build().get()) {
      AbfsInputStream spy =
          Mockito.spy((AbfsInputStream) in.getWrappedStream());
      ReadBufferManager rbm = spy.getReadBufferManager();

      Mockito.doAnswer(inv -> {
        block.await();
        return inv.callRealMethod();
      }).when(spy).readRemote(
          Mockito.anyLong(),
          Mockito.any(byte[].class),
          Mockito.anyInt(),
          Mockito.anyInt(),
          Mockito.any());

      ExecutorService exec = Executors.newFixedThreadPool(EXEC_THREADS);

      exec.submit(() -> spy.read(new byte[1], 0, 1));

      ReadBuffer inProgress = null;
      for (int i = 0; i < LOOKUP_RETRIES; i++) {
        synchronized (rbm) {
          inProgress = rbm.findInList(
              rbm.getInProgressList(), spy, 0);
        }
        if (inProgress != null) {
          break;
        }
        Thread.sleep(10);
      }

      assertNotNull(inProgress);

      Future<?> vr = exec.submit(() ->
          spy.readVectored(
              List.of(FileRange.createFileRange(ONE_MB, ONE_MB)),
              ByteBuffer::allocate));

      block.countDown();

      vr.get(FUTURE_TIMEOUT_SEC, TimeUnit.SECONDS);
      exec.shutdownNow();
    }
  }

  /**
   * Verifies vectored reads using the throughput-optimized (TPS) strategy.
   * Ensures expected backend read count and data correctness under TPS mode.
   */
  @Test
  public void testThroughputOptimizedReadVectored() throws Exception {
    Configuration conf = getRawConfiguration();
    conf.set(FS_AZURE_VECTORED_READ_STRATEGY, "TPS");

    try (AzureBlobFileSystem fs =
             (AzureBlobFileSystem) FileSystem.newInstance(conf)) {

      byte[] content = getRandomBytesArray(FILE_32_MB);
      Path path = createFileWithContent(fs, methodName.getMethodName(), content);

      List<FileRange> ranges = List.of(
          FileRange.createFileRange(0L, (int) (3.8 * ONE_MB)),
          FileRange.createFileRange((long) (4.0 * ONE_MB),
              (int) (3.2 * ONE_MB)),
          FileRange.createFileRange((long) (8.0 * ONE_MB),
              (int) (2.0 * ONE_MB)),
          FileRange.createFileRange((long) (12.0 * ONE_MB),
              (int) (4.0 * ONE_MB)),
          FileRange.createFileRange((long) (16.0 * ONE_MB),
              (int) (2.0 * ONE_MB)));

      try (FSDataInputStream in = fs.openFile(path).build().get()) {
        AbfsInputStream spy =
            Mockito.spy((AbfsInputStream) in.getWrappedStream());

        spy.readVectored(ranges, ByteBuffer::allocate);

        CompletableFuture.allOf(
            ranges.stream().map(FileRange::getData)
                .toArray(CompletableFuture[]::new)).get();

        validateVectoredReadResult(ranges, content, 0);

        Mockito.verify(spy, Mockito.times(5))
            .readRemote(
                Mockito.anyLong(),
                Mockito.any(byte[].class),
                Mockito.anyInt(),
                Mockito.anyInt(),
                Mockito.any());
      }
    }
  }
}
