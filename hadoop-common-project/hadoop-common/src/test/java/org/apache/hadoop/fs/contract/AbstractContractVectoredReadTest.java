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
import java.util.concurrent.ExecutionException;
import java.util.function.IntFunction;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileRangeImpl;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.impl.FutureIOSupport;
import org.apache.hadoop.io.WeakReferencedElasticByteBufferPool;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertDatasetEquals;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.validateVectoredReadResult;

@RunWith(Parameterized.class)
public abstract class AbstractContractVectoredReadTest extends AbstractFSContractTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractContractVectoredReadTest.class);

  public static final int DATASET_LEN = 64 * 1024;
  protected static final byte[] DATASET = ContractTestUtils.dataset(DATASET_LEN, 'a', 32);
  protected static final String VECTORED_READ_FILE_NAME = "vectored_file.txt";

  protected final IntFunction<ByteBuffer> allocate;

  private WeakReferencedElasticByteBufferPool pool = new WeakReferencedElasticByteBufferPool();

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
  public void testVectoredReadMultipleRanges() throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      FileRange fileRange = new FileRangeImpl(i * 100, 100);
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
    }
  }

  @Test
  public void testVectoredReadAndReadFully()  throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(new FileRangeImpl(100, 100));
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, allocate);
      byte[] readFullRes = new byte[100];
      in.readFully(100, readFullRes);
      ByteBuffer vecRes = FutureIOSupport.awaitFuture(fileRanges.get(0).getData());
      Assertions.assertThat(vecRes)
              .describedAs("Result from vectored read and readFully must match")
              .isEqualByComparingTo(ByteBuffer.wrap(readFullRes));
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
    fileRanges.add(new FileRangeImpl(0, 100));
    fileRanges.add(new FileRangeImpl(4 * 1024 + 101, 100));
    fileRanges.add(new FileRangeImpl(16 * 1024 + 101, 100));
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
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
    fileRanges.add(new FileRangeImpl(0, 100));
    fileRanges.add(new FileRangeImpl(4 *1024 - 101, 100));
    fileRanges.add(new FileRangeImpl(8*1024 - 101, 100));
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
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
    fileRanges.add(new FileRangeImpl(8*1024, 100));
    fileRanges.add(new FileRangeImpl(14*1024, 100));
    fileRanges.add(new FileRangeImpl(10*1024, 100));
    fileRanges.add(new FileRangeImpl(2 *1024 - 101, 100));
    fileRanges.add(new FileRangeImpl(40*1024, 1024));
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
    }
  }

  @Test
  public void testOverlappingRanges() throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = getSampleOverlappingRanges();
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
    }
  }

  @Test
  public void testSameRanges() throws Exception {
    // Same ranges are special case of overlapping only.
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = getSampleSameRanges();
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
    }
  }

  protected List<FileRange> getSampleSameRanges() {
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(new FileRangeImpl(8*1024, 1000));
    fileRanges.add(new FileRangeImpl(8*1024, 1000));
    fileRanges.add(new FileRangeImpl(8*1024, 1000));
    return fileRanges;
  }

  protected List<FileRange> getSampleOverlappingRanges() {
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(new FileRangeImpl(100, 500));
    fileRanges.add(new FileRangeImpl(400, 500));
    return fileRanges;
  }
  protected void validateUnsupportedOperation(FileSystem fs,
                                            List<? extends FileRange> fileRanges)
          throws Exception {
    CompletableFuture<FSDataInputStream> builder =
            fs.openFile(path(VECTORED_READ_FILE_NAME))
                    .build();
    try (FSDataInputStream in = builder.get()) {
      LambdaTestUtils.intercept(UnsupportedOperationException.class,
              () -> in.readVectored(fileRanges, allocate));
    }
  }

  @Test
  public void testSomeRandomNonOverlappingRanges() throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(new FileRangeImpl(500, 100));
    fileRanges.add(new FileRangeImpl(1000, 200));
    fileRanges.add(new FileRangeImpl(50, 10));
    fileRanges.add(new FileRangeImpl(10, 5));
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
    }
  }

  @Test
  public void testConsecutiveRanges() throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(new FileRangeImpl(500, 100));
    fileRanges.add(new FileRangeImpl(600, 200));
    fileRanges.add(new FileRangeImpl(800, 100));
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
    }
  }

  @Test
  public void testEOFRanges()  throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(new FileRangeImpl(DATASET_LEN, 100));
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, allocate);
      for (FileRange res : fileRanges) {
        CompletableFuture<ByteBuffer> data = res.getData();
        try {
          ByteBuffer buffer = data.get();
          // Shouldn't reach here.
          Assert.fail("EOFException must be thrown while reading EOF");
        } catch (ExecutionException ex) {
          // ignore as expected.
        } catch (Exception ex) {
          LOG.error("Exception while running vectored read ", ex);
          Assert.fail("Exception while running vectored read " + ex);
        }
      }
    }
  }

  @Test
  public void testNegativeLengthRange()  throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(new FileRangeImpl(0, -50));
    testExceptionalVectoredRead(fs, fileRanges, "Exception is expected");
  }

  @Test
  public void testNegativeOffsetRange()  throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(new FileRangeImpl(-1, 50));
    testExceptionalVectoredRead(fs, fileRanges, "Exception is expected");
  }

  @Test
  public void testNormalReadAfterVectoredRead() throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = createSomeRandomRanges();
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
    }
  }

  @Test
  public void testVectoredReadAfterNormalRead() throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = createSomeRandomRanges();
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
    }
  }

  @Test
  public void testMultipleVectoredReads() throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges1 = createSomeRandomRanges();
    List<FileRange> fileRanges2 = createSomeRandomRanges();
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges1, allocate);
      in.readVectored(fileRanges2, allocate);
      validateVectoredReadResult(fileRanges2, DATASET);
      validateVectoredReadResult(fileRanges1, DATASET);
    }
  }

  protected List<FileRange> createSomeRandomRanges() {
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(new FileRangeImpl(0, 100));
    fileRanges.add(new FileRangeImpl(110, 50));
    return fileRanges;
  }


  protected void testExceptionalVectoredRead(FileSystem fs,
                                             List<FileRange> fileRanges,
                                             String s) throws IOException {
    boolean exRaised = false;
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      // Can we intercept here as done in S3 tests ??
      in.readVectored(fileRanges, allocate);
    } catch (EOFException | IllegalArgumentException ex) {
      // expected.
      exRaised = true;
    }
    Assertions.assertThat(exRaised)
            .describedAs(s)
            .isTrue();
  }
}
