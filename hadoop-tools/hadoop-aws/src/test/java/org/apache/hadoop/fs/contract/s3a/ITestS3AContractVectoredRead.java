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

package org.apache.hadoop.fs.contract.s3a;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.contract.AbstractContractVectoredReadTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_LENGTH;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_PARQUET;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_VECTOR;
import static org.apache.hadoop.fs.contract.ContractTestUtils.range;
import static org.apache.hadoop.fs.contract.ContractTestUtils.returnBuffersToPoolPostRead;
import static org.apache.hadoop.fs.contract.ContractTestUtils.validateVectoredReadResult;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_VECTOR_READS_MAX_MERGED_READ_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_VECTOR_READS_MIN_SEEK_SIZE;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfAnalyticsAcceleratorEnabled;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.io.Sizes.S_1M;
import static org.apache.hadoop.io.Sizes.S_4K;
import static org.apache.hadoop.test.LambdaTestUtils.interceptFuture;
import static org.apache.hadoop.test.MoreAsserts.assertEqual;

/**
 * S3A contract tests for vectored reads.
 * This is a complex suite as it really is testing the store, so measurements of
 * what IO took place is also performed if the input stream is suitable for this.
 */
public class ITestS3AContractVectoredRead extends AbstractContractVectoredReadTest {

  private static final Logger LOG = LoggerFactory.getLogger(ITestS3AContractVectoredRead.class);

  public ITestS3AContractVectoredRead(String bufferType) {
    super(bufferType);
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  /**
   * Analytics Accelerator Library for Amazon S3 does not support Vectored Reads.
   * @throws Exception
   */
  @Override
  public void setup() throws Exception {
    super.setup();
    skipIfAnalyticsAcceleratorEnabled(getContract().getConf(),
        "Analytics Accelerator does not support vectored reads");
  }

  /**
   * Verify response to a vector read request which is beyond the
   * real length of the file.
   * Unlike the {@link #testEOFRanges()} test, the input stream in
   * this test thinks the file is longer than it is, so the call
   * fails in the GET request.
   */
  @Test
  public void testEOFRanges416Handling() throws Exception {
    FileSystem fs = getFileSystem();

    final int extendedLen = DATASET_LEN + 1024;
    CompletableFuture<FSDataInputStream> builder =
        fs.openFile(path(VECTORED_READ_FILE_NAME))
            .mustLong(FS_OPTION_OPENFILE_LENGTH, extendedLen)
            .opt(FS_OPTION_OPENFILE_READ_POLICY,
                FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE)
            .build();
    List<FileRange> fileRanges = range(DATASET_LEN, 100);

    // read starting past EOF generates a 416 response, mapped to
    // RangeNotSatisfiableEOFException
    describe("Read starting from past EOF");
    try (FSDataInputStream in = builder.get()) {
      in.readVectored(fileRanges, getAllocate());
      FileRange res = fileRanges.get(0);
      CompletableFuture<ByteBuffer> data = res.getData();
      interceptFuture(EOFException.class,
          "",
          ContractTestUtils.VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS,
          TimeUnit.SECONDS,
          data);
    }

    // a read starting before the EOF and continuing past it does generate
    // an EOF exception, but not a 416.
    describe("Read starting 0 continuing past EOF");
    try (FSDataInputStream in = fs.openFile(path(VECTORED_READ_FILE_NAME))
                .mustLong(FS_OPTION_OPENFILE_LENGTH, extendedLen)
                .build().get()) {
      final FileRange range = FileRange.createFileRange(0, extendedLen);
      in.readVectored(Arrays.asList(range), getAllocate());
      CompletableFuture<ByteBuffer> data = range.getData();
      interceptFuture(EOFException.class, "",
          ContractTestUtils.VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS,
          TimeUnit.SECONDS,
          data);
    }

  }

  @Test
  public void testMinSeekAndMaxSizeConfigsPropagation() throws Exception {
    Configuration conf = getFileSystem().getConf();
    S3ATestUtils.removeBaseAndBucketOverrides(conf,
            AWS_S3_VECTOR_READS_MAX_MERGED_READ_SIZE,
            AWS_S3_VECTOR_READS_MIN_SEEK_SIZE);
    S3ATestUtils.disableFilesystemCaching(conf);
    final int configuredMinSeek = 2 * 1024;
    final int configuredMaxSize = 10 * 1024 * 1024;
    conf.set(AWS_S3_VECTOR_READS_MIN_SEEK_SIZE, "2K");
    conf.set(AWS_S3_VECTOR_READS_MAX_MERGED_READ_SIZE, "10M");
    try (S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf)) {
      try (FSDataInputStream fis = openVectorFile(fs)) {
        int newMinSeek = fis.minSeekForVectorReads();
        int newMaxSize = fis.maxReadSizeForVectorReads();
        assertEqual(newMinSeek, configuredMinSeek,
                "configured s3a min seek for vectored reads");
        assertEqual(newMaxSize, configuredMaxSize,
                "configured s3a max size for vectored reads");
      }
    }
  }

  @Test
  public void testMinSeekAndMaxSizeDefaultValues() throws Exception {
    Configuration conf = getFileSystem().getConf();
    S3ATestUtils.removeBaseAndBucketOverrides(conf,
            AWS_S3_VECTOR_READS_MIN_SEEK_SIZE,
            AWS_S3_VECTOR_READS_MAX_MERGED_READ_SIZE);
    try (S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf)) {
      try (FSDataInputStream fis = openVectorFile(fs)) {
        int minSeek = fis.minSeekForVectorReads();
        int maxSize = fis.maxReadSizeForVectorReads();
        assertEqual(minSeek, Constants.DEFAULT_AWS_S3_VECTOR_READS_MIN_SEEK_SIZE,
                "default s3a min seek for vectored reads");
        assertEqual(maxSize, Constants.DEFAULT_AWS_S3_VECTOR_READS_MAX_MERGED_READ_SIZE,
                "default s3a max read size for vectored reads");
      }
    }
  }

  @Test
  public void testStopVectoredIoOperationsCloseStream() throws Exception {

    List<FileRange> fileRanges = createSampleNonOverlappingRanges();
    try (FSDataInputStream in = openVectorFile()){
      in.readVectored(fileRanges, getAllocate());
      in.close();
      LambdaTestUtils.intercept(InterruptedIOException.class,
          () -> validateVectoredReadResult(fileRanges, DATASET, 0));
    }
    // reopening the stream should succeed.
    try (FSDataInputStream in = openVectorFile()){
      in.readVectored(fileRanges, getAllocate());
      validateVectoredReadResult(fileRanges, DATASET, 0);
    }
  }

  /**
   * Verify that unbuffer() stops vectored IO operations.
   * There's a small risk of a race condition where the unbuffer() call
   * is made after the vector reads have completed.
   */
  @Test
  public void testStopVectoredIoOperationsUnbuffer() throws Exception {

    List<FileRange> fileRanges = createSampleNonOverlappingRanges();
    try (FSDataInputStream in = openVectorFile()){
      in.readVectored(fileRanges, getAllocate());
      in.unbuffer();
      LambdaTestUtils.intercept(InterruptedIOException.class,
          () -> validateVectoredReadResult(fileRanges, DATASET, 0));
      // re-initiating the vectored reads after unbuffer should succeed.
      in.readVectored(fileRanges, getAllocate());
      validateVectoredReadResult(fileRanges, DATASET, 0);
    }

  }

  /**
   * As the minimum seek value is 4*1024, the first three ranges will be
   * merged into and other two will remain as it is.
   * */
  @Test
  public void testNormalReadVsVectoredReadStatsCollection() throws Exception {

    try (S3AFileSystem fs = getTestFileSystemWithReadAheadDisabled()) {
      List<FileRange> fileRanges = new ArrayList<>();
      range(fileRanges, 10 * 1024, 100);
      range(fileRanges, 8 * 1024, 100);
      range(fileRanges, 14 * 1024, 100);
      range(fileRanges, 2 * 1024 - 101, 100);
      range(fileRanges, 40 * 1024, 1024);

      FileStatus fileStatus = fs.getFileStatus(path(VECTORED_READ_FILE_NAME));
      CompletableFuture<FSDataInputStream> builder =
          fs.openFile(path(VECTORED_READ_FILE_NAME))
              .withFileStatus(fileStatus)
              .opt(FS_OPTION_OPENFILE_READ_POLICY,
                  FS_OPTION_OPENFILE_READ_POLICY_PARQUET
                      + ", " + FS_OPTION_OPENFILE_READ_POLICY_VECTOR)
              .build();
      try (FSDataInputStream in = builder.get()) {
        in.readVectored(fileRanges, getAllocate());
        validateVectoredReadResult(fileRanges, DATASET, 0);
        returnBuffersToPoolPostRead(fileRanges, getPool());
        final InputStream wrappedStream = in.getWrappedStream();

        // policy will be random.
        if (wrappedStream instanceof S3AInputStream) {
          S3AInputStream inner = (S3AInputStream) wrappedStream;
          Assertions.assertThat(inner.getInputPolicy())
              .describedAs("Input policy of %s", inner)
              .isEqualTo(S3AInputPolicy.Random);
          Assertions.assertThat(inner.isObjectStreamOpen())
              .describedAs("Object stream open in %s", inner)
              .isFalse();
        }

        // audit the io statistics for this stream
        IOStatistics st = in.getIOStatistics();
        LOG.info("IOStats after readVectored operation {}", ioStatisticsToPrettyString(st));

        // the vectored io operation must be tracked
        verifyStatisticCounterValue(st,
                StreamStatisticNames.STREAM_READ_VECTORED_OPERATIONS,
                1);

        // the vectored io operation is being called with 5 input ranges.
        verifyStatisticCounterValue(st,
                StreamStatisticNames.STREAM_READ_VECTORED_INCOMING_RANGES,
                5);

        // 5 input ranges got combined in 3 as some of them are close.
        verifyStatisticCounterValue(st,
                StreamStatisticNames.STREAM_READ_VECTORED_COMBINED_RANGES,
                3);

        // number of bytes discarded will be based on the above input ranges.
        verifyStatisticCounterValue(st,
                StreamStatisticNames.STREAM_READ_VECTORED_READ_BYTES_DISCARDED,
                5944);

        verifyStatisticCounterValue(st,
                StoreStatisticNames.ACTION_HTTP_GET_REQUEST,
                3);

        // read bytes should match the sum of requested length for each input ranges.
        verifyStatisticCounterValue(st,
                StreamStatisticNames.STREAM_READ_BYTES,
                1424);

      }

      CompletableFuture<FSDataInputStream> builder1 =
              fs.openFile(path(VECTORED_READ_FILE_NAME))
                      .withFileStatus(fileStatus)
                      .build();

      try (FSDataInputStream in = builder1.get()) {
        for (FileRange range : fileRanges) {
          byte[] temp = new byte[range.getLength()];
          in.readFully((int) range.getOffset(), temp, 0, range.getLength());
        }

        // audit the statistics for this stream
        IOStatistics st = in.getIOStatistics();
        LOG.info("IOStats after read fully operation {}", ioStatisticsToPrettyString(st));

        verifyStatisticCounterValue(st,
                StreamStatisticNames.STREAM_READ_VECTORED_OPERATIONS,
                0);

        // all other counter values consistent.
        verifyStatisticCounterValue(st,
                StreamStatisticNames.STREAM_READ_VECTORED_READ_BYTES_DISCARDED,
                0);
        verifyStatisticCounterValue(st,
                StoreStatisticNames.ACTION_HTTP_GET_REQUEST,
                5);

        // read bytes should match the sum of requested length for each input ranges.
        verifyStatisticCounterValue(st,
                StreamStatisticNames.STREAM_READ_BYTES,
                1424);
      }
      // validate stats are getting merged at fs instance level.
      IOStatistics fsStats = fs.getIOStatistics();
      // only 1 vectored io call is made in this fs instance.
      verifyStatisticCounterValue(fsStats,
              StreamStatisticNames.STREAM_READ_VECTORED_OPERATIONS,
              1);
      // 8 get requests were made in this fs instance.
      verifyStatisticCounterValue(fsStats,
              StoreStatisticNames.ACTION_HTTP_GET_REQUEST,
              8);

      verifyStatisticCounterValue(fsStats,
              StreamStatisticNames.STREAM_READ_BYTES,
              2848);
    }
  }

  @Test
  public void testMultiVectoredReadStatsCollection() throws Exception {
    try (S3AFileSystem fs = getTestFileSystemWithReadAheadDisabled()) {
      List<FileRange> ranges1 = getConsecutiveRanges();
      List<FileRange> ranges2 = getConsecutiveRanges();
      FileStatus fileStatus = fs.getFileStatus(path(VECTORED_READ_FILE_NAME));
      CompletableFuture<FSDataInputStream> builder =
              fs.openFile(path(VECTORED_READ_FILE_NAME))
                      .withFileStatus(fileStatus)
                      .build();
      try (FSDataInputStream in = builder.get()) {
        in.readVectored(ranges1, getAllocate());
        in.readVectored(ranges2, getAllocate());
        validateVectoredReadResult(ranges1, DATASET, 0);
        validateVectoredReadResult(ranges2, DATASET, 0);
        returnBuffersToPoolPostRead(ranges1, getPool());
        returnBuffersToPoolPostRead(ranges2, getPool());

        // audit the io statistics for this stream
        IOStatistics st = in.getIOStatistics();

        // 2 vectored io calls are made above.
        verifyStatisticCounterValue(st,
                StreamStatisticNames.STREAM_READ_VECTORED_OPERATIONS,
                2);

        // 2 vectored io operation is being called with 2 input ranges.
        verifyStatisticCounterValue(st,
                StreamStatisticNames.STREAM_READ_VECTORED_INCOMING_RANGES,
                4);

        // 2 ranges are getting merged in 1 during both vectored io operation.
        verifyStatisticCounterValue(st,
                StreamStatisticNames.STREAM_READ_VECTORED_COMBINED_RANGES,
                2);

        // number of bytes discarded will be 0 as the ranges are consecutive.
        verifyStatisticCounterValue(st,
                StreamStatisticNames.STREAM_READ_VECTORED_READ_BYTES_DISCARDED,
                0);
        // only 2 http get request will be made because ranges in both range list will be merged
        // to 1 because they are consecutive.
        verifyStatisticCounterValue(st,
                StoreStatisticNames.ACTION_HTTP_GET_REQUEST,
                2);
        // read bytes should match the sum of requested length for each input ranges.
        verifyStatisticCounterValue(st,
                StreamStatisticNames.STREAM_READ_BYTES,
                2000);
      }
      IOStatistics fsStats = fs.getIOStatistics();
      // 2 vectored io calls are made in this fs instance.
      verifyStatisticCounterValue(fsStats,
              StreamStatisticNames.STREAM_READ_VECTORED_OPERATIONS,
              2);
      // 2 get requests were made in this fs instance.
      verifyStatisticCounterValue(fsStats,
              StoreStatisticNames.ACTION_HTTP_GET_REQUEST,
              2);
    }
  }

  /**
   * Create a test fs with no readahead.
   * The vector IO ranges are set to the original small values,
   * so ranges on small files are not coalesced.
   * @return a filesystem
   * @throws IOException failure to instantiate.
   */
  private S3AFileSystem getTestFileSystemWithReadAheadDisabled() throws IOException {
    Configuration conf = getFileSystem().getConf();
    // also resetting the min seek and max size values is important
    // as this same test suite has test which overrides these params.
    S3ATestUtils.removeBaseAndBucketOverrides(conf,
            Constants.READAHEAD_RANGE,
            AWS_S3_VECTOR_READS_MAX_MERGED_READ_SIZE,
            AWS_S3_VECTOR_READS_MIN_SEEK_SIZE);
    S3ATestUtils.disableFilesystemCaching(conf);
    conf.setInt(Constants.READAHEAD_RANGE, 0);
    conf.setInt(AWS_S3_VECTOR_READS_MIN_SEEK_SIZE, S_4K);
    conf.setInt(AWS_S3_VECTOR_READS_MAX_MERGED_READ_SIZE, S_1M);
    return S3ATestUtils.createTestFileSystem(conf);
  }
}
