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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.impl.FutureIOSupport;
import org.apache.hadoop.io.WeakReferencedElasticByteBufferPool;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.util.functional.FutureIO;

import static org.apache.hadoop.fs.contract.ContractTestUtils.VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertCapabilities;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertDatasetEquals;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.returnBuffersToPoolPostRead;
import static org.apache.hadoop.fs.contract.ContractTestUtils.validateVectoredReadResult;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.test.LambdaTestUtils.interceptFuture;

@RunWith(Parameterized.class)
public abstract class AbstractContractVectoredReadTest extends AbstractFSContractTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractContractVectoredReadTest.class);

  public static final int DATASET_LEN = 64 * 1024;
  protected static final byte[] DATASET = ContractTestUtils.dataset(DATASET_LEN, 'a', 32);
  protected static final String VECTORED_READ_FILE_NAME = "vectored_file.txt";

  private final IntFunction<ByteBuffer> allocate;

  private final WeakReferencedElasticByteBufferPool pool =
          new WeakReferencedElasticByteBufferPool();

  private final String bufferType;

  @Parameterized.Parameters(name = "Buffer type : {0}")
  public static List<String> params() {
    return Arrays.asList("direct", "array");
  }

  public AbstractContractVectoredReadTest(String bufferType) {
    this.bufferType = bufferType;
    this.allocate = value -> {
      boolean isDirect = !"array".equals(bufferType);
      return pool.getBuffer(isDirect, value);
    };
  }

  public IntFunction<ByteBuffer> getAllocate() {
    return allocate;
  }

  public WeakReferencedElasticByteBufferPool getPool() {
    return pool;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    Path path = path(VECTORED_READ_FILE_NAME);
    FileSystem fs = getFileSystem();
    createFile(fs, path, true, DATASET);
  }

  @Override
  public void teardown() throws Exception {
    super.teardown();
    pool.release();
  }

  @Test
  public void testVectoredReadCapability() throws Exception {
    FileSystem fs = getFileSystem();
    String[] vectoredReadCapability = new String[]{StreamCapabilities.VECTOREDIO};
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      assertCapabilities(in, vectoredReadCapability, null);
    }
  }

  @Test
  public void testVectoredReadMultipleRanges() throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      FileRange fileRange = FileRange.createFileRange(i * 100, 100);
      fileRanges.add(fileRange);
    }
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, allocate);
      CompletableFuture<?>[] completableFutures = new CompletableFuture<?>[fileRanges.size()];
      int i = 0;
      for (FileRange res : fileRanges) {
        completableFutures[i++] = res.getData();
      }
      CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(completableFutures);
      combinedFuture.get();

      validateVectoredReadResult(fileRanges, DATASET);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  @Test
  public void testVectoredReadAndReadFully()  throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(FileRange.createFileRange(100, 100));
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, allocate);
      byte[] readFullRes = new byte[100];
      in.readFully(100, readFullRes);
      ByteBuffer vecRes = FutureIOSupport.awaitFuture(fileRanges.get(0).getData());
      Assertions.assertThat(vecRes)
              .describedAs("Result from vectored read and readFully must match")
              .isEqualByComparingTo(ByteBuffer.wrap(readFullRes));
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  /**
   * As the minimum seek value is 4*1024,none of the below ranges
   * will get merged.
   */
  @Test
  public void testDisjointRanges() throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(FileRange.createFileRange(0, 100));
    fileRanges.add(FileRange.createFileRange(4_000 + 101, 100));
    fileRanges.add(FileRange.createFileRange(16_000 + 101, 100));
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  /**
   * As the minimum seek value is 4*1024, all the below ranges
   * will get merged into one.
   */
  @Test
  public void testAllRangesMergedIntoOne() throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(FileRange.createFileRange(0, 100));
    fileRanges.add(FileRange.createFileRange(4_000 - 101, 100));
    fileRanges.add(FileRange.createFileRange(8_000 - 101, 100));
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
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
    fileRanges.add(FileRange.createFileRange(8 * 1024, 100));
    fileRanges.add(FileRange.createFileRange(14 * 1024, 100));
    fileRanges.add(FileRange.createFileRange(10 * 1024, 100));
    fileRanges.add(FileRange.createFileRange(2 * 1024 - 101, 100));
    fileRanges.add(FileRange.createFileRange(40 * 1024, 1024));
    FileStatus fileStatus = fs.getFileStatus(path(VECTORED_READ_FILE_NAME));
    CompletableFuture<FSDataInputStream> builder =
            fs.openFile(path(VECTORED_READ_FILE_NAME))
                    .withFileStatus(fileStatus)
                    .build();
    try (FSDataInputStream in = builder.get()) {
      in.readVectored(fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  @Test
  public void testOverlappingRanges() throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = getSampleOverlappingRanges();
    FileStatus fileStatus = fs.getFileStatus(path(VECTORED_READ_FILE_NAME));
    CompletableFuture<FSDataInputStream> builder =
            fs.openFile(path(VECTORED_READ_FILE_NAME))
                    .withFileStatus(fileStatus)
                    .build();
    try (FSDataInputStream in = builder.get()) {
      in.readVectored(fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  @Test
  public void testSameRanges() throws Exception {
    // Same ranges are special case of overlapping only.
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = getSampleSameRanges();
    CompletableFuture<FSDataInputStream> builder =
            fs.openFile(path(VECTORED_READ_FILE_NAME))
                    .build();
    try (FSDataInputStream in = builder.get()) {
      in.readVectored(fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  @Test
  public void testSomeRandomNonOverlappingRanges() throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(FileRange.createFileRange(500, 100));
    fileRanges.add(FileRange.createFileRange(1000, 200));
    fileRanges.add(FileRange.createFileRange(50, 10));
    fileRanges.add(FileRange.createFileRange(10, 5));
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  @Test
  public void testConsecutiveRanges() throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(FileRange.createFileRange(500, 100));
    fileRanges.add(FileRange.createFileRange(600, 200));
    fileRanges.add(FileRange.createFileRange(800, 100));
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  /**
   * Test to validate EOF ranges. Default implementation fails with EOFException
   * while reading the ranges. Some implementation like s3, checksum fs fail fast
   * as they already have the file length calculated.
   */
  @Test
  public void testEOFRanges()  throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(FileRange.createFileRange(DATASET_LEN, 100));
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, allocate);
      for (FileRange res : fileRanges) {
        CompletableFuture<ByteBuffer> data = res.getData();
        interceptFuture(EOFException.class,
                "",
                ContractTestUtils.VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS,
                TimeUnit.SECONDS,
                data);
      }
    }
  }

  @Test
  public void testNegativeLengthRange()  throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(FileRange.createFileRange(0, -50));
    verifyExceptionalVectoredRead(fs, fileRanges, IllegalArgumentException.class);
  }

  @Test
  public void testNegativeOffsetRange()  throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(FileRange.createFileRange(-1, 50));
    verifyExceptionalVectoredRead(fs, fileRanges, EOFException.class);
  }

  @Test
  public void testNormalReadAfterVectoredRead() throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = createSampleNonOverlappingRanges();
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, allocate);
      // read starting 200 bytes
      byte[] res = new byte[200];
      in.read(res, 0, 200);
      ByteBuffer buffer = ByteBuffer.wrap(res);
      assertDatasetEquals(0, "normal_read", buffer, 200, DATASET);
      Assertions.assertThat(in.getPos())
              .describedAs("Vectored read shouldn't change file pointer.")
              .isEqualTo(200);
      validateVectoredReadResult(fileRanges, DATASET);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  @Test
  public void testVectoredReadAfterNormalRead() throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = createSampleNonOverlappingRanges();
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      // read starting 200 bytes
      byte[] res = new byte[200];
      in.read(res, 0, 200);
      ByteBuffer buffer = ByteBuffer.wrap(res);
      assertDatasetEquals(0, "normal_read", buffer, 200, DATASET);
      Assertions.assertThat(in.getPos())
              .describedAs("Vectored read shouldn't change file pointer.")
              .isEqualTo(200);
      in.readVectored(fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  @Test
  public void testMultipleVectoredReads() throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges1 = createSampleNonOverlappingRanges();
    List<FileRange> fileRanges2 = createSampleNonOverlappingRanges();
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges1, allocate);
      in.readVectored(fileRanges2, allocate);
      validateVectoredReadResult(fileRanges2, DATASET);
      validateVectoredReadResult(fileRanges1, DATASET);
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
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(FileRange.createFileRange(8 * 1024, 100));
    fileRanges.add(FileRange.createFileRange(14 * 1024, 100));
    fileRanges.add(FileRange.createFileRange(10 * 1024, 100));
    fileRanges.add(FileRange.createFileRange(2 * 1024 - 101, 100));
    fileRanges.add(FileRange.createFileRange(40 * 1024, 1024));

    ExecutorService dataProcessor = Executors.newFixedThreadPool(5);
    CountDownLatch countDown = new CountDownLatch(fileRanges.size());

    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, value -> pool.getBuffer(true, value));
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
    CompletableFuture<ByteBuffer> data = res.getData();
    // Read the data and perform custom operation. Here we are just
    // validating it with original data.
    FutureIO.awaitFuture(data.thenAccept(buffer -> {
      assertDatasetEquals((int) res.getOffset(),
              "vecRead", buffer, res.getLength(), DATASET);
      // return buffer to the pool once read.
      pool.putBuffer(buffer);
    }),
    VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // countdown to notify main thread that processing has been done.
    countDownLatch.countDown();
  }


  protected List<FileRange> createSampleNonOverlappingRanges() {
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(FileRange.createFileRange(0, 100));
    fileRanges.add(FileRange.createFileRange(110, 50));
    return fileRanges;
  }

  protected List<FileRange> getSampleSameRanges() {
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(FileRange.createFileRange(8_000, 1000));
    fileRanges.add(FileRange.createFileRange(8_000, 1000));
    fileRanges.add(FileRange.createFileRange(8_000, 1000));
    return fileRanges;
  }

  protected List<FileRange> getSampleOverlappingRanges() {
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(FileRange.createFileRange(100, 500));
    fileRanges.add(FileRange.createFileRange(400, 500));
    return fileRanges;
  }

  protected List<FileRange> getConsecutiveRanges() {
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(FileRange.createFileRange(100, 500));
    fileRanges.add(FileRange.createFileRange(600, 500));
    return fileRanges;
  }

  /**
   * Validate that exceptions must be thrown during a vectored
   * read operation with specific input ranges.
   * @param fs FileSystem instance.
   * @param fileRanges input file ranges.
   * @param clazz type of exception expected.
   * @throws Exception any other IOE.
   */
  protected <T extends Throwable> void verifyExceptionalVectoredRead(
          FileSystem fs,
          List<FileRange> fileRanges,
          Class<T> clazz) throws Exception {

    CompletableFuture<FSDataInputStream> builder =
            fs.openFile(path(VECTORED_READ_FILE_NAME))
                    .build();
    try (FSDataInputStream in = builder.get()) {
      intercept(clazz,
          () -> in.readVectored(fileRanges, allocate));
    }
  }
}
