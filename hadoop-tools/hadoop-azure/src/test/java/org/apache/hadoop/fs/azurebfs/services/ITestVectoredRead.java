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

import java.io.FilterInputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

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
import org.apache.hadoop.fs.azurebfs.enums.VectoredReadStrategy;
import org.apache.hadoop.fs.impl.CombinedFileRange;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_VECTORED_READ_STRATEGY;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ZERO;
import static org.apache.hadoop.fs.contract.ContractTestUtils.validateVectoredReadResult;

public class ITestVectoredRead extends AbstractAbfsIntegrationTest {
  private static final int DATA_2_MB = 2 * ONE_MB;
  private static final int DATA_4_MB = 4 * ONE_MB;
  private static final int DATA_8_MB = 8 * ONE_MB;
  private static final int DATA_10_MB = 10 * ONE_MB;
  private static final int DATA_12_MB = 12 * ONE_MB;
  private static final int DATA_16_MB = 16 * ONE_MB;
  private static final int DATA_32_MB = 32 * ONE_MB;
  private static final int DATA_100_MB = 100 * ONE_MB;
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
  private static final double MB_3_8 = 3.8;
  private static final double MB_4_0 = 4.0;
  private static final double MB_3_2 = 3.2;
  private static final double MB_8_0 = 8.0;
  private static final double MB_2_0 = 2.0;
  private static final double MB_12_0 = 12.0;
  private static final double MB_16_0 = 16.0;
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
  private static final int FUTURE_TIMEOUT_SEC = 50;
  public static final int SLEEP_TIME = 10;
  private static final long SHUFFLE_SEED = 12345L;
  private static final double PERFORMANCE_TOLERANCE_FACTOR = 1.2;

  public ITestVectoredRead() throws Exception {
  }

  /**
   * Verifies basic correctness of vectored reads using simple disjoint ranges.
   * Compares vectored read output against a full sequential read to ensure
   * data integrity is preserved.
   */
  @Test
  public void testDisjointRangesWithVectoredRead() throws Throwable {
    int fileSize = ONE_MB;
    final AzureBlobFileSystem fs = getFileSystem();
    String fileName = methodName.getMethodName() + 1;
    byte[] fileContent = getRandomBytesArray(fileSize);
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);

    List<FileRange> rangeList = new ArrayList<>();
    rangeList.add(FileRange.createFileRange(OFFSET_100_B, LEN_10K_B));
    rangeList.add(FileRange.createFileRange(OFFSET_15K_B, LEN_27K_B));
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;
    CompletableFuture<FSDataInputStream> builder = fs.openFile(testFilePath)
        .build();

    try (FSDataInputStream in = builder.get()) {
      in.readVectored(rangeList, allocate);
      byte[] readFullRes = new byte[(int) fileSize];
      in.readFully(0, readFullRes);
      // Comparing vectored read results with read fully.
      validateVectoredReadResult(rangeList, readFullRes, 0);
    }
  }

  /**
   * Ensures disjoint but mergeable ranges result in fewer backend reads.
   * Validates that vectored read coalescing reduces remote calls
   * while still returning correct data.
   */
  @Test
  public void testVectoredReadDisjointRangesExpectTwoBackendReads()
      throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    String fileName = methodName.getMethodName();
    byte[] fileContent = getRandomBytesArray(DATA_16_MB);
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);
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
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;
    try (FSDataInputStream in =
             fs.openFile(testFilePath).build().get()) {
      AbfsInputStream abfsIn = (AbfsInputStream) in.getWrappedStream();
      AbfsInputStream spyIn = Mockito.spy(abfsIn);
      spyIn.readVectored(ranges, allocate);
      CompletableFuture<?>[] futures =
          new CompletableFuture<?>[ranges.size()];
      int i = 0;
      for (FileRange range : ranges) {
        futures[i++] = range.getData();
      }
      CompletableFuture.allOf(futures).get();
      validateVectoredReadResult(ranges, fileContent, 0);
      Mockito.verify(spyIn, Mockito.times(2))
          .readRemote(
              Mockito.anyLong(),
              Mockito.any(byte[].class),
              Mockito.anyInt(),
              Mockito.anyInt(),
              Mockito.any());
    }
  }

  /**
   * Validates fallback behavior when vectored read queuing fails.
   * Ensures the implementation switches to direct reads and still
   * completes all requested ranges correctly.
   */
  @Test
  public void testVectoredReadFallsBackToDirectReadWhenQueuingFails()
      throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    String fileName = methodName.getMethodName();
    byte[] fileContent = getRandomBytesArray(DATA_4_MB);
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);

    List<FileRange> ranges = List.of(
        FileRange.createFileRange(0, ONE_MB),
        FileRange.createFileRange(DATA_2_MB, ONE_MB));
    IntFunction<ByteBuffer> allocator = ByteBuffer::allocate;

    try (FSDataInputStream in = fs.openFile(testFilePath).build().get()) {
      AbfsInputStream abfsIn = (AbfsInputStream) in.getWrappedStream();
      AbfsInputStream spyIn = Mockito.spy(abfsIn);
      VectoredReadHandler realHandler = abfsIn.getVectoredReadHandler();
      VectoredReadHandler spyHandler = Mockito.spy(realHandler);
      Mockito.doReturn(spyHandler).when(spyIn).getVectoredReadHandler();
      Mockito.doReturn(false)
          .when(spyHandler)
          .queueVectoredRead(
              Mockito.any(AbfsInputStream.class),
              Mockito.any(CombinedFileRange.class),
              ArgumentMatchers.<IntFunction<ByteBuffer>>any());
      spyIn.readVectored(ranges, allocator);
      CompletableFuture<?>[] futures
          = new CompletableFuture<?>[ranges.size()];
      for (int i = 0; i < ranges.size(); i++) {
        futures[i] = ranges.get(i).getData();
      }
      CompletableFuture.allOf(futures).get();
      Mockito.verify(spyHandler, Mockito.atLeastOnce())
          .directRead(
              Mockito.any(AbfsInputStream.class),
              Mockito.any(CombinedFileRange.class),
              Mockito.eq(allocator));

      validateVectoredReadResult(ranges, fileContent, 0);
    }
  }

  /**
   * Tests vectored read correctness with multiple non-contiguous ranges.
   * Confirms that all ranges are read correctly even when more than two
   * disjoint segments are requested.
   */
  @Test
  public void testMultipleDisjointRangesWithVectoredRead() throws Throwable {
    int fileSize = ONE_MB;
    final AzureBlobFileSystem fs = getFileSystem();
    String fileName = methodName.getMethodName() + 1;
    byte[] fileContent = getRandomBytesArray(fileSize);
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);

    List<FileRange> ranges = List.of(
        FileRange.createFileRange(OFFSET_100_B, LEN_10K_B),
        FileRange.createFileRange(OFFSET_15K_B, LEN_27K_B),
        FileRange.createFileRange(OFFSET_42K_B, LEN_40K_B));
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;
    CompletableFuture<FSDataInputStream> builder = fs.openFile(testFilePath)
        .build();

    try (FSDataInputStream in = builder.get()) {
      in.readVectored(ranges, allocate);
      byte[] readFullRes = new byte[(int) fileSize];
      in.readFully(0, readFullRes);
      // Comparing vectored read results with read fully.
      validateVectoredReadResult(ranges, readFullRes, 0);
    }
  }

  /**
   * Exercises vectored reads on a large file with many scattered ranges.
   * Ensures correctness and stability of vectored read logic under
   * high-offset and large-file conditions.
   */
  @Test
  public void testVectoredIOHugeFile() throws Throwable {
    int fileSize = DATA_100_MB;
    final AzureBlobFileSystem fs = getFileSystem();
    String fileName = methodName.getMethodName() + 1;
    byte[] fileContent = getRandomBytesArray(fileSize);
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);

    List<FileRange> ranges = List.of(
        FileRange.createFileRange(HUGE_OFFSET_1, HUGE_RANGE),
        FileRange.createFileRange(HUGE_OFFSET_2, HUGE_RANGE),
        FileRange.createFileRange(HUGE_OFFSET_3, HUGE_RANGE),
        FileRange.createFileRange(HUGE_OFFSET_4, HUGE_RANGE),
        FileRange.createFileRange(HUGE_OFFSET_5, HUGE_RANGE),
        FileRange.createFileRange(HUGE_OFFSET_6, HUGE_RANGE),
        FileRange.createFileRange(HUGE_OFFSET_7, HUGE_RANGE_LARGE));
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    CompletableFuture<FSDataInputStream> builder =
        fs.openFile(testFilePath).build();
    try (FSDataInputStream in = builder.get()) {
      in.readVectored(ranges, allocate);
      byte[] readFullRes = new byte[(int) fileSize];
      in.readFully(0, readFullRes);
      // Comparing vectored read results with read fully.
      validateVectoredReadResult(ranges, readFullRes, 0);
    }
  }

  /**
   * Verifies that vectored reads and sequential reads can execute concurrently.
   * Ensures correct behavior when prefetch and vectored I/O overlap in time.
   */
  @Test
  public void testSimultaneousPrefetchAndVectoredRead() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    String fileName = methodName.getMethodName();
    byte[] fileContent = getRandomBytesArray(DATA_16_MB);
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);
    try (FSDataInputStream in = fs.openFile(testFilePath).build().get()) {
      AbfsInputStream abfsIn = (AbfsInputStream) in.getWrappedStream();
      IntFunction<ByteBuffer> allocator = ByteBuffer::allocate;
      List<FileRange> vRanges = new ArrayList<>();
      vRanges.add(FileRange.createFileRange(DATA_10_MB, (int) ONE_MB));
      vRanges.add(FileRange.createFileRange(DATA_12_MB, (int) ONE_MB));
      byte[] seqBuffer = new byte[(int) ONE_MB];
      CountDownLatch latch = new CountDownLatch(1);
      CompletableFuture<Void> vectoredTask = CompletableFuture.runAsync(() -> {
        try {
          latch.await();
          abfsIn.readVectored(vRanges, allocator);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      CompletableFuture<Void> sequentialTask = CompletableFuture.runAsync(
          () -> {
            try {
              latch.await();
              abfsIn.read(0, seqBuffer, 0, (int) ONE_MB);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
      latch.countDown();
      CompletableFuture.allOf(vectoredTask, sequentialTask).get();
      CompletableFuture<?>[] vFutures = vRanges.stream()
          .map(FileRange::getData)
          .toArray(CompletableFuture[]::new);
      CompletableFuture.allOf(vFutures).get();
      assertArrayEquals(Arrays.copyOfRange(fileContent, 0, (int) ONE_MB),
          seqBuffer, "Sequential read data mismatch");
      validateVectoredReadResult(vRanges, fileContent, 0);
    }
  }

  /**
   * Tests concurrent access using separate streams on different files.
   * Ensures vectored reads on one file do not interfere with sequential
   * reads and readahead on another file.
   */
  @Test
  public void testConcurrentStreamsOnDifferentFiles() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    // Create two distinct files with random content
    byte[] content1 = getRandomBytesArray(DATA_16_MB);
    byte[] content2 = getRandomBytesArray(DATA_16_MB);
    Path path1 = createFileWithContent(fs, "file1", content1);
    Path path2 = createFileWithContent(fs, "file2", content2);

    // Open two separate input streams for concurrent access
    try (FSDataInputStream in1 = fs.openFile(path1).build().get();
         FSDataInputStream in2 = fs.openFile(path2).build().get()) {

      AbfsInputStream streamVectored = (AbfsInputStream) in1.getWrappedStream();
      AbfsInputStream streamSequential
          = (AbfsInputStream) in2.getWrappedStream();
      IntFunction<ByteBuffer> allocator = ByteBuffer::allocate;

      // Define non-contiguous ranges for the vectored read on file 1
      List<FileRange> ranges = List.of(
          FileRange.createFileRange(DATA_2_MB, ONE_MB),
          FileRange.createFileRange(DATA_4_MB, ONE_MB));

      // Use a latch to ensure both threads start their I/O at the same time
      CountDownLatch latch = new CountDownLatch(1);

      // Thread 1: Perform asynchronous vectored reads on file 1
      CompletableFuture<Void> vectoredTask = CompletableFuture.runAsync(() -> {
        try {
          latch.await();
          streamVectored.readVectored(ranges, allocator);
        } catch (Exception e) {
          throw new RuntimeException("Vectored read task failed", e);
        }
      });

      // Thread 2: Perform multiple sequential reads on file 2 to trigger readahead
      CompletableFuture<Void> sequentialTask = CompletableFuture.runAsync(
          () -> {
            try {
              latch.await();
              for (int i = 0; i < SEQ_READ_ITERATIONS; i++) {
                byte[] tempBuf = new byte[(int) ONE_MB];
                streamSequential.read(i * ONE_MB, tempBuf, 0, (int) ONE_MB);
                // Validate data integrity for file 2 immediately
                assertArrayEquals(Arrays.copyOfRange(content2, i * (int) ONE_MB,
                        (i + 1) * (int) ONE_MB), tempBuf,
                    "Sequential read mismatch in file 2 at block " + i);
              }
            } catch (Exception e) {
              throw new RuntimeException("Sequential read task failed", e);
            }
          });
      // Trigger simultaneous execution
      latch.countDown();
      // Wait for both high-level tasks to finish
      CompletableFuture.allOf(vectoredTask, sequentialTask).get();

      // Explicitly wait for the vectored read futures to complete their data transfer
      CompletableFuture<?>[] vFutures = ranges.stream()
          .map(FileRange::getData)
          .toArray(CompletableFuture[]::new);
      CompletableFuture.allOf(vFutures).get();

      // Final validation of vectored read content for file 1
      validateVectoredReadResult(ranges, content1, 0);
    }
  }

  @Test
  public void testVectoredReadHitchhikesOnExistingPrefetch() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    String fileName = methodName.getMethodName();
    byte[] fileContent = getRandomBytesArray(DATA_8_MB);
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);

    try (FSDataInputStream in = fs.openFile(testFilePath).build().get()) {
      AbfsInputStream abfsIn = (AbfsInputStream) in.getWrappedStream();

      // Since client is final in AbfsInputStream, we cannot inject a spy into it.
      // Instead, spy on abfsIn itself and stub readRemote to delegate to the real
      // implementation while still being tracked by Mockito.
      AbfsInputStream spyIn = Mockito.spy(abfsIn);
      Mockito.doCallRealMethod().when(spyIn).readRemote(
          Mockito.anyLong(),
          Mockito.any(byte[].class),
          Mockito.anyInt(),
          Mockito.anyInt(),
          Mockito.any());

      // Replace the wrapped stream inside FSDataInputStream with our spy,
      // so all subsequent calls go through spyIn.
      Field wrappedField = FilterInputStream.class.getDeclaredField("in");
      wrappedField.setAccessible(true);
      wrappedField.set(in, spyIn);

      // 1. Trigger sequential read → starts readahead covering [0, readAheadSize).
      byte[] seqBuf = new byte[1];
      in.read(seqBuf, 0, 1);

      // 2. Queue a vectored read fully inside the readahead window.
      List<FileRange> vRanges = new ArrayList<>();
      vRanges.add(FileRange.createFileRange(ONE_MB, (int) ONE_MB));
      IntFunction<ByteBuffer> allocator = ByteBuffer::allocate;
      in.readVectored(vRanges, allocator);

      // 3. Wait for completion.
      vRanges.get(0).getData().get();

      // 4. Validate data integrity.
      validateVectoredReadResult(vRanges, fileContent, ZERO);

      // 5. THE CRITICAL VALIDATION:
      // Max 2 remote reads acceptable:
      //   - Read #1: readahead triggered by the sequential read
      //   - Read #2: only if vectored read just missed the prefetch window (race edge)
      // 3+ means hitchhiking is broken — vectored read issued a redundant remote fetch.
      final int maxExpectedRemoteReads = 2;
      Mockito.verify(spyIn, Mockito.atMost(maxExpectedRemoteReads))
          .readRemote(
              Mockito.anyLong(),
              Mockito.any(byte[].class),
              Mockito.anyInt(),
              Mockito.anyInt(),
              Mockito.any());
    }
  }

  @Test
  public void testMultipleReadsWhileBufferInProgressEventuallyComplete() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    String fileName = methodName.getMethodName();
    byte[] fileContent = getRandomBytesArray(DATA_8_MB);
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);
    CountDownLatch blockCompletion = new CountDownLatch(1);

    try (FSDataInputStream in = fs.openFile(testFilePath).build().get()) {
      AbfsInputStream abfsIn = (AbfsInputStream) in.getWrappedStream();
      AbfsInputStream spyIn = Mockito.spy(abfsIn);

      // Inject spy into FSDataInputStream so all calls go through spyIn,
      // including those from background threads spawned during read().
      Field wrappedField = FilterInputStream.class.getDeclaredField("in");
      wrappedField.setAccessible(true);
      wrappedField.set(in, spyIn);

      ReadBufferManager rbm = spyIn.getReadBufferManager();
      AtomicBoolean firstCall = new AtomicBoolean(true);

      Mockito.doAnswer(invocation -> {
            if (firstCall.getAndSet(false)) {
              blockCompletion.await();
            }
            return invocation.callRealMethod();
          })
          .when(spyIn)
          .readRemote(Mockito.anyLong(), Mockito.any(byte[].class),
              Mockito.anyInt(), Mockito.anyInt(), Mockito.any());

      ExecutorService exec = Executors.newFixedThreadPool(EXEC_THREADS);
      try {
        // r1 triggers a readahead; the first readRemote call will block on
        // blockCompletion, simulating an in-progress buffer.
        Future<?> r1 = exec.submit(() -> {
          try {
            in.read(new byte[1], 0, 1);  // use 'in', not 'spyIn' directly
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

        // Poll until the buffer appears in the inProgressList.
        ReadBuffer inProgress = null;
        for (int i = 0; i < LOOKUP_RETRIES; i++) {
          synchronized (rbm) {
            inProgress = rbm.findInList(rbm.getInProgressList(), spyIn, 0);
          }
          if (inProgress != null) {
            break;
          }
          Thread.sleep(SLEEP_TIME);
        }
        assertNotNull(inProgress,
            "Expected buffer to be in inProgressList while completion is blocked");

        // r2 reads the same offset — should wait on the in-progress buffer,
        // not issue a new remote read.
        Future<?> r2 = exec.submit(() -> {
          try {
            in.read(new byte[1], 0, 1);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

        // Vectored read targeting the same in-progress buffer range —
        // should hitchhike rather than issue a new remote read.
        long bufferOffset = inProgress.getOffset();
        int length = (int) Math.min(ONE_MB, DATA_8_MB - bufferOffset);
        List<FileRange> ranges = new ArrayList<>();
        ranges.add(FileRange.createFileRange(bufferOffset, length));
        Future<?> vr = exec.submit(() -> {
          try {
            in.readVectored(ranges, ByteBuffer::allocate);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

        // Give r2 and vr time to reach their wait state before unblocking.
        Thread.sleep(FUTURE_TIMEOUT_SEC);
        blockCompletion.countDown();

        r1.get(FUTURE_TIMEOUT_SEC, TimeUnit.SECONDS);
        r2.get(FUTURE_TIMEOUT_SEC, TimeUnit.SECONDS);
        vr.get(FUTURE_TIMEOUT_SEC, TimeUnit.SECONDS);
        ranges.get(0).getData().get(FUTURE_TIMEOUT_SEC, TimeUnit.SECONDS);

        validateVectoredReadResult(ranges, fileContent, bufferOffset);
      } finally {
        exec.shutdownNow();
        // Restore original stream reference to avoid affecting shared state.
        wrappedField.set(in, abfsIn);
      }
    }
  }

  /**
   * Verifies vectored read behavior under throughput-optimized strategy.
   * Confirms that ranges are split as expected and that the number of
   * backend reads matches the throughput-oriented execution model.
   */
  @Test
  public void testThroughputOptimizedReadVectored() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.set(FS_AZURE_VECTORED_READ_STRATEGY,
        VectoredReadStrategy.THROUGHPUT_OPTIMIZED.getName());
    FileSystem fileSystem = FileSystem.newInstance(configuration);
    try (AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem) {
      String fileName = methodName.getMethodName();
      byte[] fileContent = getRandomBytesArray(DATA_32_MB);
      Path testFilePath = createFileWithContent(abfs, fileName, fileContent);
      List<FileRange> fileRanges = new ArrayList<>();
      fileRanges.add(
          FileRange.createFileRange(0L, (int) (MB_3_8 * ONE_MB)));
      fileRanges.add(
          FileRange.createFileRange(
              (long) (MB_4_0 * ONE_MB), (int) (MB_3_2 * ONE_MB)));
      fileRanges.add(
          FileRange.createFileRange(
              (long) (MB_8_0 * ONE_MB), (int) (MB_2_0 * ONE_MB)));
      fileRanges.add(
          FileRange.createFileRange(
              (long) (MB_12_0 * ONE_MB), (int) (MB_4_0 * ONE_MB)));
      fileRanges.add(
          FileRange.createFileRange(
              (long) (MB_16_0 * ONE_MB), (int) (MB_2_0 * ONE_MB)));
      IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;
      try (FSDataInputStream in =
               abfs.openFile(testFilePath).build().get()) {
        AbfsInputStream abfsIn = (AbfsInputStream) in.getWrappedStream();
        AbfsInputStream spyIn = Mockito.spy(abfsIn);
        spyIn.readVectored(fileRanges, allocate);
        CompletableFuture<?>[] futures =
            new CompletableFuture<?>[fileRanges.size()];
        int i = 0;
        for (FileRange range : fileRanges) {
          futures[i++] = range.getData();
        }
        CompletableFuture.allOf(futures).get();
        validateVectoredReadResult(fileRanges, fileContent, 0);
        Mockito.verify(spyIn, Mockito.times(5))
            .readRemote(
                Mockito.anyLong(),
                Mockito.any(byte[].class),
                Mockito.anyInt(),
                Mockito.anyInt(),
                Mockito.any());
      }
    }
  }

  /**
   * Compares performance of many random reads using:
   * 1. Individual random (non-vectored) reads
   * 2. A single vectored read covering the same offsets
   *
   * All non-vectored reads complete first, followed by vectored reads.
   * This is a relative performance test intended to catch regressions,
   * not to assert absolute timing guarantees.
   */
  @Test
  public void testRandomReadsNonVectoredThenVectoredPerformance()
      throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final int fileSize = 128 * ONE_MB;
    final int readSize = 64 * ONE_KB;
    final int readCount = 512;
    byte[] fileContent = getRandomBytesArray(fileSize);
    Path testPath =
        createFileWithContent(fs, methodName.getMethodName(), fileContent);
    /* ----------------------------------------------------
     * Generate NON-overlapping offsets (shuffled)
     * ---------------------------------------------------- */
    List<Long> offsets = new ArrayList<>();
    for (long offset = 0; offset + readSize <= fileSize; offset += readSize) {
      offsets.add(offset);
    }
    // Shuffle to simulate randomness without overlap
    Collections.shuffle(offsets, new Random(SHUFFLE_SEED));
    // Limit to readCount
    offsets = offsets.subList(0, Math.min(readCount, offsets.size()));

    /* ----------------------------------------------------
     * Build vectored ranges
     * ---------------------------------------------------- */
    List<FileRange> vectoredRanges = new ArrayList<>();
    for (long offset : offsets) {
      vectoredRanges.add(
          FileRange.createFileRange(offset, readSize));
    }
    try (FSDataInputStream in = fs.openFile(testPath).build().get()) {

      /* ----------------------------------------------------
       * Phase 1: Random non-vectored reads
       * ---------------------------------------------------- */
      byte[] buffer = new byte[readSize];
      long nonVectoredStartNs = System.nanoTime();
      for (long offset : offsets) {
        in.seek(offset);
        in.read(buffer, 0, readSize);
      }
      long nonVectoredTimeNs =
          System.nanoTime() - nonVectoredStartNs;
      /* ----------------------------------------------------
       * Phase 2: Vectored reads
       * ---------------------------------------------------- */
      long vectoredStartNs = System.nanoTime();
      in.readVectored(vectoredRanges, ByteBuffer::allocate);
      CompletableFuture.allOf(
          vectoredRanges.stream()
              .map(FileRange::getData)
              .toArray(CompletableFuture[]::new)
      ).get();
      long vectoredTimeNs =
          System.nanoTime() - vectoredStartNs;
      /* ----------------------------------------------------
       * Assertion (less flaky)
       * ---------------------------------------------------- */
      assertTrue(
          vectoredTimeNs <= nonVectoredTimeNs * PERFORMANCE_TOLERANCE_FACTOR,
          String.format(
              "Vectored read slower: vectored=%d ns, non-vectored=%d ns",
              vectoredTimeNs, nonVectoredTimeNs)
      );
    }
  }
}
