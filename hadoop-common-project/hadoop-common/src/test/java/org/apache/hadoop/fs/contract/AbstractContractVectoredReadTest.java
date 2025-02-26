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

package org.apache.hadoop.fs.contract;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.io.WeakReferencedElasticByteBufferPool;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static java.util.Arrays.asList;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_LENGTH;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_VECTOR;
import static org.apache.hadoop.fs.contract.ContractTestUtils.VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertDatasetEquals;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.range;
import static org.apache.hadoop.fs.contract.ContractTestUtils.returnBuffersToPoolPostRead;
import static org.apache.hadoop.fs.contract.ContractTestUtils.validateVectoredReadResult;
  import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.test.LambdaTestUtils.interceptFuture;
import static org.apache.hadoop.util.functional.FutureIO.awaitFuture;

/**
 * Test Vectored Reads.
 * <p>
 * Both the original readVectored(allocator) and the readVectored(allocator, release)
 * operations are tested.
 */
@RunWith(Parameterized.class)
public abstract class AbstractContractVectoredReadTest extends AbstractFSContractTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractContractVectoredReadTest.class);

  public static final int DATASET_LEN = 64 * 1024;
  protected static final byte[] DATASET = ContractTestUtils.dataset(DATASET_LEN, 'a', 32);
  protected static final String VECTORED_READ_FILE_NAME = "vectored_file.txt";

  /**
   * Buffer allocator for vector IO.
   */
  private final IntFunction<ByteBuffer> allocate;

  /**
   * Buffer pool for vector IO.
   */
  private final ElasticByteBufferPool pool =
          new WeakReferencedElasticByteBufferPool();

  private final String bufferType;

  /**
   * Path to the vector file.
   */
  private Path vectorPath;

  /**
   * Counter of buffer releases.
   * Because not all implementations release buffers on failures,
   * this is not yet used in assertions.
   */
  private final AtomicInteger bufferReleases = new AtomicInteger();

  @Parameterized.Parameters(name = "Buffer type : {0}")
  public static List<String> params() {
    return asList("direct", "array");
  }

  protected AbstractContractVectoredReadTest(String bufferType) {
    this.bufferType = bufferType;
    final boolean isDirect = !"array".equals(bufferType);
    this.allocate = size -> pool.getBuffer(isDirect, size);
  }

  /**
   * Get the buffer allocator.
   * @return allocator function for vector IO.
   */
  protected IntFunction<ByteBuffer> getAllocate() {
    return allocate;
  }

  /**
   * The buffer release operation.
   */
  protected void release(ByteBuffer buffer) {
    LOG.info("Released buffer {}", buffer);
    bufferReleases.incrementAndGet();
    pool.putBuffer(buffer);
  }

  /**
   * Get the vector IO buffer pool.
   * @return a pool.
   */

  protected ElasticByteBufferPool getPool() {
    return pool;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    vectorPath = path(VECTORED_READ_FILE_NAME);
    FileSystem fs = getFileSystem();
    createFile(fs, vectorPath, true, DATASET);
  }

  @Override
  public void teardown() throws Exception {
    pool.release();
    super.teardown();
  }

  /**
   * Open the vector file.
   * @return the input stream.
   * @throws IOException failure.
   */
  protected FSDataInputStream openVectorFile() throws IOException {
    return openVectorFile(getFileSystem());
  }

  /**
   * Open the vector file.
   * @param fs filesystem to use
   * @return the input stream.
   * @throws IOException failure.
   */
  protected FSDataInputStream openVectorFile(final FileSystem fs) throws IOException {
    return awaitFuture(
        fs.openFile(vectorPath)
            .opt(FS_OPTION_OPENFILE_LENGTH, DATASET_LEN)
            .opt(FS_OPTION_OPENFILE_READ_POLICY,
                FS_OPTION_OPENFILE_READ_POLICY_VECTOR)
            .build());
  }

  @Test
  public void testVectoredReadMultipleRanges() throws Exception {
    List<FileRange> fileRanges = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      FileRange fileRange = FileRange.createFileRange(i * 100, 100);
      fileRanges.add(fileRange);
    }
    try (FSDataInputStream in = openVectorFile()) {
      in.readVectored(fileRanges, allocate, this::release);
      CompletableFuture<?>[] completableFutures = new CompletableFuture<?>[fileRanges.size()];
      int i = 0;
      for (FileRange res : fileRanges) {
        completableFutures[i++] = res.getData();
      }
      CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(completableFutures);
      combinedFuture.get();

      validateVectoredReadResult(fileRanges, DATASET, 0);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  @Test
  public void testVectoredReadAndReadFully()  throws Exception {
    List<FileRange> fileRanges = new ArrayList<>();
    range(fileRanges, 100, 100);
    try (FSDataInputStream in = openVectorFile()) {
      in.readVectored(fileRanges, allocate);
      byte[] readFullRes = new byte[100];
      in.readFully(100, readFullRes);
      ByteBuffer vecRes = awaitFuture(fileRanges.get(0).getData());
      Assertions.assertThat(vecRes)
              .describedAs("Result from vectored read and readFully must match")
              .isEqualByComparingTo(ByteBuffer.wrap(readFullRes));
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  @Test
  public void testVectoredReadWholeFile()  throws Exception {
    describe("Read the whole file in one single vectored read");
    List<FileRange> fileRanges = new ArrayList<>();
    range(fileRanges, 0, DATASET_LEN);
    try (FSDataInputStream in = openVectorFile()) {
      in.readVectored(fileRanges, allocate);
      ByteBuffer vecRes = awaitFuture(fileRanges.get(0).getData());
      Assertions.assertThat(vecRes)
              .describedAs("Result from vectored read and readFully must match")
              .isEqualByComparingTo(ByteBuffer.wrap(DATASET));
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  /**
   * As the minimum seek value is 4*1024,none of the below ranges
   * will get merged.
   */
  @Test
  public void testDisjointRanges() throws Exception {
    List<FileRange> fileRanges = new ArrayList<>();
    range(fileRanges, 0, 100);
    range(fileRanges, 4_000 + 101, 100);
    range(fileRanges, 16_000 + 101, 100);
    try (FSDataInputStream in = openVectorFile()) {
      in.readVectored(fileRanges, allocate, this::release);
      validateVectoredReadResult(fileRanges, DATASET, 0);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  /**
   * As the minimum seek value is 4*1024, all the below ranges
   * will get merged into one.
   */
  @Test
  public void testAllRangesMergedIntoOne() throws Exception {
    List<FileRange> fileRanges = new ArrayList<>();
    final int length = 100;
    range(fileRanges, 0, length);
    range(fileRanges, 4_000 - length - 1, length);
    range(fileRanges, 8_000 - length - 1, length);
    try (FSDataInputStream in = openVectorFile()) {
      in.readVectored(fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET, 0);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  /**
   * As the minimum seek value is 4*1024, the first three ranges will be
   * merged into and other two will remain as it is.
   */
  @Test
  public void testSomeRangesMergedSomeUnmerged() throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    range(fileRanges, 8 * 1024, 100);
    range(fileRanges, 14 * 1024, 100);
    range(fileRanges, 10 * 1024, 100);
    range(fileRanges, 2 * 1024 - 101, 100);
    range(fileRanges, 40 * 1024, 1024);
    FileStatus fileStatus = fs.getFileStatus(path(VECTORED_READ_FILE_NAME));
    CompletableFuture<FSDataInputStream> builder =
            fs.openFile(path(VECTORED_READ_FILE_NAME))
                    .withFileStatus(fileStatus)
                    .build();
    try (FSDataInputStream in = builder.get()) {
      in.readVectored(fileRanges, allocate, this::release);
      validateVectoredReadResult(fileRanges, DATASET, 0);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  /**
   * Most file systems won't support overlapping ranges.
   * Currently, only Raw Local supports it.
   */
  @Test
  public void testOverlappingRanges() throws Exception {
    if (!isSupported(VECTOR_IO_OVERLAPPING_RANGES)) {
      verifyExceptionalVectoredRead(
              getSampleOverlappingRanges(),
              IllegalArgumentException.class);
    } else {
      try (FSDataInputStream in = openVectorFile()) {
        List<FileRange> fileRanges = getSampleOverlappingRanges();
        in.readVectored(fileRanges, allocate);
        validateVectoredReadResult(fileRanges, DATASET, 0);
        returnBuffersToPoolPostRead(fileRanges, pool);
      }
    }
  }

  /**
   * Same ranges are special case of overlapping.
   */
  @Test
  public void testSameRanges() throws Exception {
    if (!isSupported(VECTOR_IO_OVERLAPPING_RANGES)) {
      verifyExceptionalVectoredRead(
              getSampleSameRanges(),
              IllegalArgumentException.class);
    } else {
      try (FSDataInputStream in = openVectorFile()) {
        List<FileRange> fileRanges = getSampleSameRanges();
        in.readVectored(fileRanges, allocate, this::release);
        validateVectoredReadResult(fileRanges, DATASET, 0);
        returnBuffersToPoolPostRead(fileRanges, pool);
      }
    }
  }

  /**
   * A null range is not permitted.
   */
  @Test
  public void testNullRange() throws Exception {
    List<FileRange> fileRanges = new ArrayList<>();
    range(fileRanges, 500, 100);
    fileRanges.add(null);
    verifyExceptionalVectoredRead(
        fileRanges,
        NullPointerException.class);
  }
  /**
   * A null range is not permitted.
   */
  @Test
  public void testNullRangeList() throws Exception {
    verifyExceptionalVectoredRead(
        null,
        NullPointerException.class);
  }

  @Test
  public void testSomeRandomNonOverlappingRanges() throws Exception {
    List<FileRange> fileRanges = new ArrayList<>();
    range(fileRanges, 500, 100);
    range(fileRanges, 1000, 200);
    range(fileRanges, 50, 10);
    range(fileRanges, 10, 5);
    try (FSDataInputStream in = openVectorFile()) {
      in.readVectored(fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET, 0);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  @Test
  public void testConsecutiveRanges() throws Exception {
    List<FileRange> fileRanges = new ArrayList<>();
    final int offset = 500;
    final int length = 2011;
    range(fileRanges, offset, length);
    range(fileRanges, offset + length, length);
    try (FSDataInputStream in = openVectorFile()) {
      in.readVectored(fileRanges, allocate, this::release);
      validateVectoredReadResult(fileRanges, DATASET, 0);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  @Test
  public void testEmptyRanges() throws Exception {
    List<FileRange> fileRanges = new ArrayList<>();
    try (FSDataInputStream in = openVectorFile()) {
      in.readVectored(fileRanges, allocate);
      Assertions.assertThat(fileRanges)
          .describedAs("Empty ranges must stay empty")
          .isEmpty();
    }
  }

  /**
   * Test to validate EOF ranges.
   * <p>
   * Default implementation fails with EOFException
   * while reading the ranges. Some implementation like s3, checksum fs fail fast
   * as they already have the file length calculated.
   * The contract option {@link ContractOptions#VECTOR_IO_EARLY_EOF_CHECK} is used
   * to determine which check to perform.
   */
  @Test
  public void testEOFRanges()  throws Exception {
    describe("Testing reading with an offset past the end of the file");
    List<FileRange> fileRanges = range(DATASET_LEN + 1, 100);

    if (isSupported(VECTOR_IO_EARLY_EOF_CHECK)) {
      LOG.info("Expecting early EOF failure");
      verifyExceptionalVectoredRead(fileRanges, EOFException.class);
    } else {
      expectEOFinRead(fileRanges);
    }
  }


  @Test
  public void testVectoredReadWholeFilePlusOne()  throws Exception {
    describe("Try to read whole file plus 1 byte");
    List<FileRange> fileRanges = range(0, DATASET_LEN + 1);

    if (isSupported(VECTOR_IO_EARLY_EOF_CHECK)) {
      LOG.info("Expecting early EOF failure");
      verifyExceptionalVectoredRead(fileRanges, EOFException.class);
    } else {
      expectEOFinRead(fileRanges);
    }
  }

  private void expectEOFinRead(final List<FileRange> fileRanges) throws Exception {
    LOG.info("Expecting late EOF failure");
    try (FSDataInputStream in = openVectorFile()) {
      in.readVectored(fileRanges, allocate, this::release);
      for (FileRange res : fileRanges) {
        CompletableFuture<ByteBuffer> data = res.getData();
        interceptFuture(EOFException.class,
            "",
            VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS,
            TimeUnit.SECONDS,
            data);
      }
    }
  }

  @Test
  public void testNegativeLengthRange()  throws Exception {

    verifyExceptionalVectoredRead(range(0, -50), IllegalArgumentException.class);
  }

  @Test
  public void testNegativeOffsetRange()  throws Exception {
    verifyExceptionalVectoredRead(range(-1, 50), EOFException.class);
  }

  @Test
  public void testNullReleaseOperation()  throws Exception {

    final List<FileRange> range = range(0, 10);

    try (FSDataInputStream in = openVectorFile()) {
      intercept(NullPointerException.class, () ->
          in.readVectored(range, allocate, null));
    }
  }

  @Test
  public void testNormalReadAfterVectoredRead() throws Exception {
    List<FileRange> fileRanges = createSampleNonOverlappingRanges();
    try (FSDataInputStream in = openVectorFile()) {
      in.readVectored(fileRanges, allocate);
      // read starting 200 bytes
      final int len = 200;
      byte[] res = new byte[len];
      in.readFully(res, 0, len);
      ByteBuffer buffer = ByteBuffer.wrap(res);
      assertDatasetEquals(0, "normal_read", buffer, len, DATASET);
      validateVectoredReadResult(fileRanges, DATASET, 0);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  @Test
  public void testVectoredReadAfterNormalRead() throws Exception {
    List<FileRange> fileRanges = createSampleNonOverlappingRanges();
    try (FSDataInputStream in = openVectorFile()) {
      // read starting 200 bytes
      final int len = 200;
      byte[] res = new byte[len];
      in.readFully(res, 0, len);
      ByteBuffer buffer = ByteBuffer.wrap(res);
      assertDatasetEquals(0, "normal_read", buffer, len, DATASET);
      in.readVectored(fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET, 0);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  @Test
  public void testMultipleVectoredReads() throws Exception {
    List<FileRange> fileRanges1 = createSampleNonOverlappingRanges();
    List<FileRange> fileRanges2 = createSampleNonOverlappingRanges();
    try (FSDataInputStream in = openVectorFile()) {
      in.readVectored(fileRanges1, allocate);
      in.readVectored(fileRanges2, allocate, this::release);
      validateVectoredReadResult(fileRanges2, DATASET, 0);
      validateVectoredReadResult(fileRanges1, DATASET, 0);
      returnBuffersToPoolPostRead(fileRanges1, pool);
      returnBuffersToPoolPostRead(fileRanges2, pool);
    }
  }

  /**
   * This test creates list of ranges and then submit a readVectored
   * operation and then uses a separate thread pool to process the
   * results asynchronously.
   */
  @Test
  public void testVectoredIOEndToEnd() throws Exception {
    List<FileRange> fileRanges = new ArrayList<>();
    range(fileRanges, 8 * 1024, 100);
    range(fileRanges, 14 * 1024, 100);
    range(fileRanges, 10 * 1024, 100);
    range(fileRanges, 2 * 1024 - 101, 100);
    range(fileRanges, 40 * 1024, 1024);

    ExecutorService dataProcessor = Executors.newFixedThreadPool(5);
    CountDownLatch countDown = new CountDownLatch(fileRanges.size());

    try (FSDataInputStream in = openVectorFile()) {
      in.readVectored(fileRanges, this.allocate);
      for (FileRange res : fileRanges) {
        dataProcessor.submit(() -> {
          try {
            readBufferValidateDataAndReturnToPool(res, countDown);
          } catch (Exception e) {
            String error = String.format("Error while processing result for %s", res);
            LOG.error(error, e);
            ContractTestUtils.fail(error, e);
          }
        });
      }
      // user can perform other computations while waiting for IO.
      if (!countDown.await(VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        ContractTestUtils.fail("Timeout/Error while processing vectored io results");
      }
    } finally {
      HadoopExecutors.shutdown(dataProcessor, LOG,
              VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
  }

  private void readBufferValidateDataAndReturnToPool(FileRange res,
                                                     CountDownLatch countDownLatch)
          throws IOException, TimeoutException {
    try {
      CompletableFuture<ByteBuffer> data = res.getData();
      // Read the data and perform custom operation. Here we are just
      // validating it with original data.
      awaitFuture(data.thenAccept(buffer -> {
        assertDatasetEquals((int) res.getOffset(),
                "vecRead", buffer, res.getLength(), DATASET);
        // return buffer to the pool once read.
        // If the read failed, this doesn't get invoked.
        pool.putBuffer(buffer);
      }),
          VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } finally {
      // countdown to notify main thread that processing has been done.
      countDownLatch.countDown();
    }
  }


  protected List<FileRange> createSampleNonOverlappingRanges() {
    List<FileRange> fileRanges = new ArrayList<>();
    range(fileRanges, 0, 100);
    range(fileRanges, 110, 50);
    return fileRanges;
  }

  protected List<FileRange> getSampleSameRanges() {
    List<FileRange> fileRanges = new ArrayList<>();
    range(fileRanges, 8_000, 1000);
    range(fileRanges, 8_000, 1000);
    range(fileRanges, 8_000, 1000);
    return fileRanges;
  }

  protected List<FileRange> getSampleOverlappingRanges() {
    List<FileRange> fileRanges = new ArrayList<>();
    range(fileRanges, 100, 500);
    range(fileRanges, 400, 500);
    return fileRanges;
  }

  protected List<FileRange> getConsecutiveRanges() {
    List<FileRange> fileRanges = new ArrayList<>();
    range(fileRanges, 100, 500);
    range(fileRanges, 600, 500);
    return fileRanges;
  }

  /**
   * Validate that exceptions must be thrown during a vectored
   * read operation with specific input ranges.
   * @param fileRanges input file ranges.
   * @param clazz type of exception expected.
   * @throws Exception any other exception.
   */
  protected <T extends Throwable> void verifyExceptionalVectoredRead(
          List<FileRange> fileRanges,
          Class<T> clazz) throws Exception {

    try (FSDataInputStream in = openVectorFile()) {
      intercept(clazz, () -> {
        in.readVectored(fileRanges, allocate);
        return "triggered read of " + fileRanges.size() + " ranges" + " against " + in;
      });
    }
  }
}
