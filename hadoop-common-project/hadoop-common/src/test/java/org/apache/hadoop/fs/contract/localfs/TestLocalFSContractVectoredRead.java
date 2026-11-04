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

package org.apache.hadoop.fs.contract.localfs;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.IntFunction;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputChecker;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.contract.AbstractContractVectoredReadTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.impl.TrackingByteBufferPool;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;

import static java.util.Collections.singletonList;

import static org.apache.hadoop.fs.contract.ContractTestUtils.VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS;
import static org.apache.hadoop.fs.contract.ContractTestUtils.validateVectoredReadResult;
import static org.apache.hadoop.fs.StreamCapabilities.VECTOREDIO_BUFFERS_SLICED;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticCounter;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BYTES;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_VECTORED_OPERATIONS;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.test.LambdaTestUtils.interceptFuture;
import static org.apache.hadoop.test.LambdaTestUtils.eventually;
import static org.assertj.core.api.Assertions.assertThat;

@ParameterizedClass(name = "buffer-{0}")
@MethodSource("params")
public class TestLocalFSContractVectoredRead extends AbstractContractVectoredReadTest {

  private long initialBytesRead;

  private final boolean isDirect;

  public TestLocalFSContractVectoredRead(final String bufferType) {
    super(bufferType);
    isDirect = !"array".equals(bufferType);
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new LocalFSContract(conf);
  }

  @Test
  public void testChecksumValidationDuringVectoredRead() throws Exception {
    Path testPath = path("big_range_checksum_file");
    List<FileRange> someRandomRanges = new ArrayList<>();
    someRandomRanges.add(FileRange.createFileRange(10, 1024));
    someRandomRanges.add(FileRange.createFileRange(1040, 1024));
    validateCheckReadException(testPath, DATASET_LEN, someRandomRanges);
  }


  /**
   * Test for file size less than checksum chunk size.
   * {@code ChecksumFileSystem#bytesPerChecksum}.
   */
  @Test
  public void testChecksumValidationDuringVectoredReadSmallFile() throws Exception {
    Path testPath = path("big_range_checksum_file");
    final int length = 471;
    List<FileRange> smallFileRanges = new ArrayList<>();
    smallFileRanges.add(FileRange.createFileRange(10, 50));
    smallFileRanges.add(FileRange.createFileRange(100, 20));
    validateCheckReadException(testPath, length, smallFileRanges);
  }

  /**
   * Verify that checksum validation works through vectored reads.
   * @param testPath path to the file to be tested
   * @param length length of the file to be created
   * @param ranges ranges to be read from the file
   * @throws Exception any exception other than ChecksumException
   */
  private void validateCheckReadException(Path testPath,
      int length,
      List<FileRange> ranges) throws Exception {
    LocalFileSystem localFs = (LocalFileSystem) getFileSystem();
    final byte[] datasetCorrect = ContractTestUtils.dataset(length, 'a', 32);
    try (FSDataOutputStream out = localFs.create(testPath, true)) {
      out.write(datasetCorrect);
    }
    Path checksumPath = localFs.getChecksumFile(testPath);
    Assertions.assertThat(localFs.exists(checksumPath))
        .describedAs("Checksum file should be present")
        .isTrue();
    CompletableFuture<FSDataInputStream> fis = localFs.openFile(testPath).build();
    try (FSDataInputStream in = fis.get()) {
      in.readVectored(ranges, getAllocate());
      validateVectoredReadResult(ranges, datasetCorrect, 0);
    }
    final byte[] datasetCorrupted = ContractTestUtils.dataset(length, 'a', 64);
    try (FSDataOutputStream out = localFs.getRaw().create(testPath, true)) {
      out.write(datasetCorrupted);
    }
    CompletableFuture<FSDataInputStream> fisN = localFs.openFile(testPath).build();
    try (FSDataInputStream in = fisN.get()) {
      in.readVectored(ranges, getAllocate());
      // Expect checksum exception when data is updated directly through
      // raw local fs instance.
      intercept(ChecksumException.class,
          () -> validateVectoredReadResult(ranges, datasetCorrupted, 0));
    }
  }

  @Test
  public void tesChecksumVectoredReadBoundaries() throws Exception {
    Path testPath = path("boundary_range_checksum_file");
    final int length = 1071;
    LocalFileSystem localFs = (LocalFileSystem) getFileSystem();
    final byte[] datasetCorrect = ContractTestUtils.dataset(length, 'a', 32);
    try (FSDataOutputStream out = localFs.create(testPath, true)) {
      out.write(datasetCorrect);
    }
    Path checksumPath = localFs.getChecksumFile(testPath);
    Assertions.assertThat(localFs.exists(checksumPath))
        .describedAs("Checksum file should be present at {} ", checksumPath)
        .isTrue();
    CompletableFuture<FSDataInputStream> fis = localFs.openFile(testPath).build();
    List<FileRange> smallRange = new ArrayList<>();
    smallRange.add(FileRange.createFileRange(1000, 71));
    try (FSDataInputStream in = fis.get()) {
      in.readVectored(smallRange, getAllocate());
      validateVectoredReadResult(smallRange, datasetCorrect, 0);
    }
  }

  /**
   * subclass so that the bytes read count can be cached before the test run.
   */
  @Test
  @Override
  public void testVectoredReadMultipleRanges() throws Exception {
    initialBytesRead = getBytesRead();
    super.testVectoredReadMultipleRanges();
  }

  /**
   * Validate statistics.
   * Sometimes the tests failed with more than expected read, so the assertions are on
   * {@code isGreaterThanOrEqualTo()} rather than exact values.
   */
  @Override
  protected void assertionsWithinTestVectoredReadMultipleRanges(
      final FSDataInputStream in,
      final List<FileRange> fileRanges) {

    // check the iostats
    final long totalVectorReadLength = fileRanges.stream().mapToLong(FileRange::getLength).sum();
    final IOStatistics stats = in.getIOStatistics();
    assertThatStatisticCounter(stats, STREAM_READ_VECTORED_OPERATIONS)
        .describedAs(STREAM_READ_VECTORED_OPERATIONS + " stream %s", stats)
        .isEqualTo(1);
    assertThatStatisticCounter(stats, STREAM_READ_BYTES)
        .describedAs(STREAM_READ_BYTES + " in bytes read in stream %s", stats)
        .isGreaterThanOrEqualTo(totalVectorReadLength);

    // validate filesystem stats, went up by at least that amount.
    // expect counting of other things, crc files in particular
    long currentBytesRead = getBytesRead();
    assertThat(currentBytesRead)
        .describedAs("bytes read in stream %s", in)
        .isGreaterThanOrEqualTo(initialBytesRead + totalVectorReadLength);
  }

  /**
   * API is deprecated, but Spark uses it, and it's how the regression was found.
   * this is how the production code looks at our stats.
   * @return counter of bytes read across all stores. Never reset.
   */
  private static long getBytesRead() {
    AtomicLong bytes = new AtomicLong();
    FileSystem.getAllStatistics().forEach(st -> bytes.addAndGet(st.getBytesRead()));
    return bytes.get();
  }

  /**
   * HADOOP-19901: Verify that checksum buffers allocated during vectored read
   * are released exactly once after verification and can be reused for a smaller
   * checksum read at EOF.
   */
  @Test
  public void testChecksumBuffersReleasedAfterVectoredRead() throws Exception {
    Path testPath = path("checksum_buffer_release_test");
    LocalFileSystem localFs = (LocalFileSystem) getFileSystem();
    final int length = 8192;
    final byte[] dataset = ContractTestUtils.dataset(length, 'a', 32);
    try (FSDataOutputStream out = localFs.create(testPath, true)) {
      out.write(dataset);
    }

    // Keep caller-owned buffers out of the pool until their results are consumed.
    AtomicLong allocations = new AtomicLong();
    AtomicLong systemReleases = new AtomicLong();
    ElasticByteBufferPool innerPool = new ElasticByteBufferPool();
    IntFunction<ByteBuffer> allocate = size -> {
      allocations.incrementAndGet();
      return innerPool.getBuffer(isDirect, size);
    };
    CountDownLatch internalReleases = new CountDownLatch(4);

    List<FileRange> ranges = new ArrayList<>();
    ranges.add(FileRange.createFileRange(0, 1024));
    ranges.add(FileRange.createFileRange(4096, 1024));

    CompletableFuture<FSDataInputStream> fis = localFs.openFile(testPath).build();
    try (FSDataInputStream in = fis.get()) {
      in.readVectored(ranges, allocate, buffer -> {
        innerPool.putBuffer(buffer);
        systemReleases.incrementAndGet();
        internalReleases.countDown();
      });
      for (FileRange range : ranges) {
        range.getData().get(VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }
      validateVectoredReadResult(ranges, dataset, 0);
      assertThat(internalReleases.await(5, TimeUnit.SECONDS))
          .describedAs("Internal data and checksum buffers released after verification and copying")
          .isTrue();
      assertThat(allocations.get())
          .describedAs("Two caller buffers, two internal data buffers and two checksum buffers")
          .isEqualTo(6);
      assertThat(systemReleases.get())
          .describedAs("Only the four internal buffers released by the system")
          .isEqualTo(4);

      List<FileRange> tailRanges = new ArrayList<>();
      tailRanges.add(FileRange.createFileRange(7680, 512));
      CountDownLatch tailInternalReleases = new CountDownLatch(2);
      in.readVectored(tailRanges, size -> {
        ByteBuffer buffer = allocate.apply(size);
        if (size == 4) {
          assertThat(buffer.capacity())
              .describedAs("Reused checksum buffer is larger than the requested range")
              .isEqualTo(8);
        }
        return buffer;
      }, buffer -> {
        innerPool.putBuffer(buffer);
        systemReleases.incrementAndGet();
        tailInternalReleases.countDown();
      });
      tailRanges.get(0).getData().get(
          VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      validateVectoredReadResult(tailRanges, dataset, 0);
      assertThat(tailInternalReleases.await(5, TimeUnit.SECONDS))
          .describedAs("Tail internal buffers released after verification and copying")
          .isTrue();
      assertThat(systemReleases.get()).isEqualTo(6);
    }
  }

  @Test
  public void testChecksumBuffersReleasedOnChecksumFailure() throws Exception {
    Path testPath = path("checksum_buffer_release_failure_test");
    LocalFileSystem localFs = (LocalFileSystem) getFileSystem();
    final int length = 8192;
    try (FSDataOutputStream out = localFs.create(testPath, true)) {
      out.write(ContractTestUtils.dataset(length, 'a', 32));
    }
    // Change the data without updating the checksum file.
    final byte[] corrupted = ContractTestUtils.dataset(length, 'a', 64);
    try (FSDataOutputStream out = localFs.getRaw().create(testPath, true)) {
      out.write(corrupted);
    }

    List<FileRange> ranges = new ArrayList<>();
    ranges.add(FileRange.createFileRange(0, 1024));
    ranges.add(FileRange.createFileRange(4096, 1024));
    try (FSDataInputStream in = localFs.open(testPath)) {
      verifyFailedReadReleasesBuffers(in, ranges, ChecksumException.class, 6);
    }
  }

  @Test
  public void testChecksumBuffersReleasedOnDataReadFailure() throws Exception {
    Path testPath = path("checksum_buffer_release_data_failure_test");
    LocalFileSystem localFs = (LocalFileSystem) getFileSystem();
    final byte[] dataset = ContractTestUtils.dataset(8192, 'a', 32);
    try (FSDataOutputStream out = localFs.create(testPath, true)) {
      out.write(dataset);
    }

    try (FSDataInputStream in = localFs.open(testPath)) {
      // Cache the file length before truncating only the data file, so validation
      // succeeds and the asynchronous data reads fail while checksum reads succeed.
      List<FileRange> initialRange = singletonList(FileRange.createFileRange(0, 512));
      in.readVectored(initialRange, getAllocate());
      validateVectoredReadResult(initialRange, dataset, 0);
      localFs.getRaw().create(testPath, true).close();

      List<FileRange> ranges = new ArrayList<>();
      ranges.add(FileRange.createFileRange(0, 1024));
      ranges.add(FileRange.createFileRange(4096, 1024));
      verifyFailedReadReleasesBuffers(in, ranges, EOFException.class, 6);
    }
  }

  @Test
  public void testBuffersReleasedOnChecksumReadFailure() throws Exception {
    Path testPath = createBufferReleaseTestFile("checksum_read_failure");
    LocalFileSystem localFs = (LocalFileSystem) getFileSystem();
    try (FSDataInputStream in = localFs.open(testPath)) {
      // Truncate after opening, so reading the header succeeds but the async
      // checksum reads fail. The raw reader must release those buffers only once.
      localFs.getRaw().create(localFs.getChecksumFile(testPath), true).close();
      verifyFailedReadReleasesBuffers(in, bufferReleaseRanges(), EOFException.class, 6);
    }
  }

  @Test
  public void testBuffersReleasedWithoutChecksumsOnReadFailure() throws Exception {
    Path testPath = createBufferReleaseTestFile("missing_checksum_read_failure");
    LocalFileSystem localFs = (LocalFileSystem) getFileSystem();
    assertThat(localFs.getRaw().delete(localFs.getChecksumFile(testPath), false)).isTrue();
    try (FSDataInputStream in = localFs.open(testPath)) {
      verifyFailedReadReleasesBuffers(in,
          singletonList(FileRange.createFileRange(8192, 512)), EOFException.class, 1);
    }
  }

  @Test
  public void testVectoredReadWithoutChecksumsPreservesBuffers() throws Exception {
    Path testPath = createBufferReleaseTestFile("missing_checksum_read_success");
    LocalFileSystem localFs = (LocalFileSystem) getFileSystem();
    assertThat(localFs.getRaw().delete(localFs.getChecksumFile(testPath), false)).isTrue();
    try (TrackingByteBufferPool tracker = newTrackingPool();
         FSDataInputStream in = localFs.open(testPath)) {
      List<FileRange> ranges = bufferReleaseRanges();
      in.readVectored(ranges, size -> tracker.getBuffer(isDirect, size), tracker::putBuffer);
      validateVectoredReadResult(ranges, ContractTestUtils.dataset(8192, 'a', 32), 0);
      assertThat(tracker.getBufferAllocations()).isEqualTo(2);
      assertThat(tracker.getBufferReleases()).isZero();
      for (FileRange range : ranges) {
        ByteBuffer buffer = range.getData().join();
        assertThat(tracker.containsBuffer(buffer)).isTrue();
        tracker.putBuffer(buffer);
      }
    }
  }

  @Test
  public void testSuccessfulBuffersPreservedOnChecksumFailure() throws Exception {
    Path testPath = createBufferReleaseTestFile("mixed_checksum_read_results");
    LocalFileSystem localFs = (LocalFileSystem) getFileSystem();
    byte[] dataset = ContractTestUtils.dataset(8192, 'a', 32);
    byte[] corrupted = dataset.clone();
    corrupted[4096] ^= 1;
    try (FSDataOutputStream out = localFs.getRaw().create(testPath, true)) {
      out.write(corrupted);
    }
    try (TrackingByteBufferPool tracker = newTrackingPool();
         FSDataInputStream in = localFs.open(testPath)) {
      CountDownLatch bufferReleases = new CountDownLatch(5);
      List<FileRange> ranges = bufferReleaseRanges();
      in.readVectored(ranges, size -> tracker.getBuffer(isDirect, size),
          releaser(tracker, bufferReleases));
      ByteBuffer successful = ranges.get(0).getData().get(5, TimeUnit.SECONDS);
      interceptFuture(ChecksumException.class, "", 5, TimeUnit.SECONDS, ranges.get(1).getData());
      assertThat(bufferReleases.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(tracker.getBufferAllocations()).isEqualTo(6);
      assertThat(tracker.getBufferReleases()).isEqualTo(5);
      assertThat(tracker.size()).isEqualTo(1);
      assertThat(tracker.containsBuffer(successful)).isTrue();
      // Validate after error cleanup, so releasing/reusing the successful buffer
      // would be detected. Return the exact allocated buffer after consuming it.
      validateVectoredReadResult(singletonList(ranges.get(0)), dataset, 0);
      tracker.putBuffer(successful);
    }
  }

  @Test
  public void testBuffersReleasedOnSynchronousDataReadFailure() throws Exception {
    verifySynchronousReadFailure(false, false);
    verifySynchronousReadFailure(false, true);
  }

  @Test
  public void testBuffersReleasedOnSynchronousChecksumReadFailure() throws Exception {
    verifySynchronousReadFailure(true, false);
    verifySynchronousReadFailure(true, true);
  }

  @Test
  public void testBuffersReleasedOnDataAllocationFailure() throws Exception {
    verifyAllocationFailure(6, false);
  }

  @Test
  public void testBuffersReleasedOnChecksumAllocationFailure() throws Exception {
    verifyAllocationFailure(4, false);
  }

  @Test
  public void testBuffersReleasedOnCallerAllocationFailure() throws Exception {
    verifyAllocationFailure(2, false);
  }

  @Test
  public void testBuffersReleasedOnAllocationError() throws Exception {
    verifyAllocationFailure(2, true);
    verifyAllocationFailure(4, true);
    verifyAllocationFailure(6, true);
  }

  @Test
  public void testBuffersReleasedOnSynchronousReadError() throws Exception {
    for (boolean checksumReadFails : new boolean[]{false, true}) {
      for (boolean completeImmediately : new boolean[]{false, true}) {
        verifySynchronousReadFailure(checksumReadFails, completeImmediately,
            new OutOfMemoryError("Injected submission error"), false);
      }
    }
  }

  @Test
  public void testCleanupFailuresLoggedAfterSynchronousReadFailure() throws Exception {
    LogCapturer logs = LogCapturer.captureLogs(FSInputChecker.LOG);
    try {
      verifySynchronousReadFailure(false, false,
          new IOException("Injected synchronous failure"), true);
      assertThat(logs.getOutput()).contains("Failed to release vectored read buffer",
          "Injected cleanup failure");
    } finally {
      logs.stopCapturing();
    }
  }

  @Test
  public void testInternalCleanupFailuresLoggedAfterSuccessfulRead() throws Exception {
    Path testPath = createBufferReleaseTestFile("internal_cleanup_failure");
    LogCapturer logs = LogCapturer.captureLogs(FSInputChecker.LOG);
    try (TrackingByteBufferPool tracker = newTrackingPool();
         FSDataInputStream in = getFileSystem().open(testPath)) {
      CountDownLatch bufferReleases = new CountDownLatch(4);
      List<FileRange> ranges = bufferReleaseRanges();
      in.readVectored(ranges, size -> tracker.getBuffer(isDirect, size), buffer -> {
        tracker.putBuffer(buffer);
        bufferReleases.countDown();
        String type = buffer.capacity() == 8 ? "checksum" : "data";
        throw new IllegalStateException("Injected " + type + " cleanup failure");
      });
      validateVectoredReadResult(ranges, ContractTestUtils.dataset(8192, 'a', 32), 0);
      assertThat(bufferReleases.await(5, TimeUnit.SECONDS)).isTrue();
      eventually(5000, 50, () -> assertThat(logs.getOutput())
          .contains("Failed to release vectored read buffer",
              "Injected checksum cleanup failure", "Injected data cleanup failure"));
      assertThat(tracker.getBufferReleases()).isEqualTo(4);
      for (FileRange range : ranges) {
        tracker.putBuffer(range.getData().join());
      }
    } finally {
      logs.stopCapturing();
    }
  }

  @Test
  public void testAllocatedBuffersReturnedWithoutSlicing() throws Exception {
    verifyAllocatedBuffersReturned(false);
    verifyAllocatedBuffersReturned(true);
  }

  @Test
  public void testCopyFailureReleasesOnlyFailedCallerBuffer() throws Exception {
    Path testPath = createBufferReleaseTestFile("copy_failure");
    ElasticByteBufferPool pool = new ElasticByteBufferPool() {
      @Override
      public ByteBuffer getBuffer(boolean direct, int length) {
        ByteBuffer buffer = super.getBuffer(direct, length);
        return length == 17 ? buffer.asReadOnlyBuffer() : buffer;
      }
    };
    try (TrackingByteBufferPool tracker = TrackingByteBufferPool.wrap(pool);
         FSDataInputStream in = getFileSystem().open(testPath)) {
      List<FileRange> ranges = List.of(FileRange.createFileRange(1, 31),
          FileRange.createFileRange(40, 17));
      CountDownLatch bufferReleases = new CountDownLatch(5);
      in.readVectored(ranges, size -> tracker.getBuffer(isDirect, size),
          releaser(tracker, bufferReleases));
      interceptFuture(ReadOnlyBufferException.class, "", 5, TimeUnit.SECONDS,
          ranges.get(1).getData());
      assertThat(bufferReleases.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(tracker.getBufferAllocations()).isEqualTo(6);
      assertThat(tracker.getBufferReleases()).isEqualTo(5);
      validateVectoredReadResult(singletonList(ranges.get(0)),
          ContractTestUtils.dataset(8192, 'a', 32), 0);
      tracker.putBuffer(ranges.get(0).getData().join());
    }
  }

  private void verifyAllocatedBuffersReturned(boolean withRelease) throws Exception {
    Path testPath = createBufferReleaseTestFile("exact_buffer_identity_" + withRelease);
    // Include unaligned ranges sharing a checksum chunk and a tail range at EOF.
    List<FileRange> ranges = List.of(FileRange.createFileRange(1, 31),
        FileRange.createFileRange(40, 17), FileRange.createFileRange(8000, 192));
    try (TrackingByteBufferPool tracker = newTrackingPool();
         FSDataInputStream in = getFileSystem().open(testPath)) {
      assertThat(in.hasCapability(VECTOREDIO_BUFFERS_SLICED)).isFalse();
      assertThat(getFileSystem().hasPathCapability(testPath, VECTOREDIO_BUFFERS_SLICED))
          .isFalse();
      Thread caller = Thread.currentThread();
      List<ByteBuffer> allocations = new ArrayList<>();
      IntFunction<ByteBuffer> allocate = size -> {
        assertThat(Thread.currentThread()).isSameAs(caller);
        // Exercise elastic allocators returning buffers larger than requested.
        ByteBuffer buffer = tracker.getBuffer(isDirect, size + 16);
        allocations.add(buffer);
        return buffer;
      };
      CountDownLatch internalReleases = new CountDownLatch(6);
      if (withRelease) {
        in.readVectored(ranges, allocate, releaser(tracker, internalReleases));
        assertThat(internalReleases.await(5, TimeUnit.SECONDS)).isTrue();
      } else {
        // A capturing allocator on the original API can release every allocation
        // itself; no sliced backing buffers need special handling.
        in.readVectored(ranges, allocate);
      }
      validateVectoredReadResult(ranges, ContractTestUtils.dataset(8192, 'a', 32), 0);
      for (int i = 0; i < ranges.size(); i++) {
        ByteBuffer result = ranges.get(i).getData().join();
        assertThat(result).isSameAs(allocations.get(i));
        assertThat(result.isDirect()).isEqualTo(isDirect);
        assertThat(tracker.containsBuffer(result)).isTrue();
      }
      if (withRelease) {
        assertThat(tracker.getBufferReleases()).isEqualTo(6);
        for (FileRange range : ranges) {
          tracker.putBuffer(range.getData().join());
        }
      } else {
        assertThat(tracker.getBufferReleases()).isZero();
        allocations.forEach(tracker::putBuffer);
      }
      assertThat(tracker.size()).isZero();
    }
  }

  private Path createBufferReleaseTestFile(String name) throws IOException {
    Path testPath = path(name);
    try (FSDataOutputStream out = getFileSystem().create(testPath, true)) {
      out.write(ContractTestUtils.dataset(8192, 'a', 32));
    }
    return testPath;
  }

  private static List<FileRange> bufferReleaseRanges() {
    return List.of(FileRange.createFileRange(0, 1024), FileRange.createFileRange(4096, 1024));
  }

  private static TrackingByteBufferPool newTrackingPool() {
    return TrackingByteBufferPool.wrap(new ElasticByteBufferPool());
  }

  private static Consumer<ByteBuffer> releaser(TrackingByteBufferPool tracker,
      CountDownLatch bufferReleases) {
    return buffer -> {
      tracker.putBuffer(buffer);
      bufferReleases.countDown();
    };
  }

  private static void assertAllBuffersReleased(TrackingByteBufferPool tracker,
      CountDownLatch bufferReleases, int expected) throws InterruptedException {
    assertThat(bufferReleases.await(5, TimeUnit.SECONDS))
        .describedAs("All allocated buffers released exactly once")
        .isTrue();
    assertThat(tracker.getBufferAllocations()).isEqualTo(expected);
    assertThat(tracker.getBufferReleases()).isEqualTo(expected);
    assertThat(tracker.size()).isZero();
  }

  private void verifyFailedReadReleasesBuffers(FSDataInputStream in, List<FileRange> ranges,
      Class<? extends IOException> exception, int expected) throws Exception {
    try (TrackingByteBufferPool tracker = newTrackingPool()) {
      CountDownLatch bufferReleases = new CountDownLatch(expected);
      in.readVectored(ranges, size -> tracker.getBuffer(isDirect, size),
          releaser(tracker, bufferReleases));
      for (FileRange range : ranges) {
        interceptFuture(exception, "", 5, TimeUnit.SECONDS, range.getData());
      }
      assertAllBuffersReleased(tracker, bufferReleases, expected);
    }
  }

  private void verifyAllocationFailure(int allocationToFail, boolean fatal) throws Exception {
    Path testPath = createBufferReleaseTestFile("allocation_failure_" + allocationToFail);
    int expected = allocationToFail - 1;
    try (TrackingByteBufferPool tracker = newTrackingPool();
         FSDataInputStream in = getFileSystem().open(testPath)) {
      CountDownLatch bufferReleases = new CountDownLatch(expected);
      Throwable failure = fatal ? new OutOfMemoryError("Direct buffer memory")
          : new IllegalStateException("Injected allocation failure");
      List<FileRange> ranges = bufferReleaseRanges();
      assertThat(intercept(failure.getClass(), () -> in.readVectored(ranges, size -> {
        if (tracker.getBufferAllocations() == expected) {
          if (fatal) {
            throw (Error) failure;
          }
          throw (RuntimeException) failure;
        }
        return tracker.getBuffer(isDirect, size);
      }, releaser(tracker, bufferReleases))))
          .isSameAs(failure);
      for (FileRange range : ranges) {
        assertThat(intercept(ExecutionException.class,
            () -> range.getData().get(5, TimeUnit.SECONDS)).getCause())
            .isSameAs(failure);
      }
      assertAllBuffersReleased(tracker, bufferReleases, expected);
    }
  }

  private void verifySynchronousReadFailure(boolean checksumReadFails,
      boolean completeImmediately) throws Exception {
    verifySynchronousReadFailure(checksumReadFails, completeImmediately,
        new IOException("Injected synchronous read failure"), false);
  }

  private void verifySynchronousReadFailure(boolean checksumReadFails,
      boolean completeImmediately, Throwable failure, boolean releaseFails) throws Exception {
    List<Runnable> completions = new ArrayList<>();
    RawLocalFileSystem rawFs = new RawLocalFileSystem() {
      @Override
      public FSDataInputStream open(Path file, int bufferSize) throws IOException {
        return new FSDataInputStream(super.open(file, bufferSize)) {
          @Override
          public void readVectored(List<? extends FileRange> ranges,
              IntFunction<ByteBuffer> allocate, Consumer<ByteBuffer> release) throws IOException {
            for (FileRange range : ranges) {
              range.setData(new CompletableFuture<>());
            }
            for (int i = 0; i < ranges.size(); i++) {
              FileRange range = ranges.get(i);
              ByteBuffer buffer = allocate.apply(range.getLength());
              Runnable complete = () -> range.getData().complete(buffer);
              if (completeImmediately) {
                complete.run();
              } else {
                completions.add(complete);
              }
              if (ChecksumFileSystem.isChecksumFile(file) == checksumReadFails) {
                // Simulate partial submission: this range has a read in flight,
                // but subsequent ranges have no allocated buffers.
                for (int j = i + 1; j < ranges.size(); j++) {
                  ranges.get(j).getData().completeExceptionally(failure);
                }
                if (failure instanceof Error) {
                  throw (Error) failure;
                }
                throw (IOException) failure;
              }
            }
          }
        };
      }
    };
    Path testPath = createBufferReleaseTestFile(
        "sync_failure_" + checksumReadFails + "_" + completeImmediately);
    int expected = checksumReadFails ? 3 : 5;
    try (LocalFileSystem localFs = new LocalFileSystem(rawFs);
         TrackingByteBufferPool tracker = newTrackingPool()) {
      localFs.initialize(getFileSystem().getUri(), getFileSystem().getConf());
      try (FSDataInputStream in = localFs.open(testPath)) {
        CountDownLatch bufferReleases = new CountDownLatch(expected);
        List<FileRange> ranges = bufferReleaseRanges();
        assertThat(intercept(failure.getClass(), () -> in.readVectored(ranges,
            size -> tracker.getBuffer(isDirect, size), buffer -> {
              releaser(tracker, bufferReleases).accept(buffer);
              if (releaseFails) {
                throw new IllegalStateException("Injected cleanup failure");
              }
            })))
            .isSameAs(failure);
        for (FileRange range : ranges) {
          assertThat(intercept(ExecutionException.class,
              () -> range.getData().get(5, TimeUnit.SECONDS)).getCause()).isSameAs(failure);
        }
        assertThat(tracker.getBufferReleases()).isEqualTo(completeImmediately ? expected : 2);
        completions.forEach(Runnable::run);
        assertAllBuffersReleased(tracker, bufferReleases, expected);
      }
    }
  }
}
