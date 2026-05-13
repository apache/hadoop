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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractVectoredReadTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.statistics.IOStatistics;

import static org.apache.hadoop.fs.contract.ContractTestUtils.validateVectoredReadResult;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticCounter;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BYTES;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_VECTORED_OPERATIONS;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;

@ParameterizedClass(name = "buffer-{0}")
@MethodSource("params")
public class TestLocalFSContractVectoredRead extends AbstractContractVectoredReadTest {

  private long initialBytesRead;

  public TestLocalFSContractVectoredRead(final String bufferType) {
    super(bufferType);
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
}
