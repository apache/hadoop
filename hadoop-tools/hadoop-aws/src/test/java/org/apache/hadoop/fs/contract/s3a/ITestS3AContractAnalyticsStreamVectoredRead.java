/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract.s3a;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.contract.AbstractContractVectoredReadTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;
import org.apache.hadoop.test.tags.IntegrationTest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.contract.ContractTestUtils.validateVectoredReadResult;
import static org.apache.hadoop.fs.s3a.Constants.ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.AAL_CACHE_TIMEOUT;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.AAL_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.AAL_REQUEST_COALESCE_TOLERANCE;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.AAL_SMALL_OBJECT_PREFETCH_ENABLED;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.enableAnalyticsAccelerator;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipForAnyEncryptionExceptSSES3;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_HTTP_GET_REQUEST;
import static org.apache.hadoop.io.Sizes.S_16K;
import static org.apache.hadoop.io.Sizes.S_1K;
import static org.apache.hadoop.io.Sizes.S_32K;


/**
 * S3A contract tests for vectored reads with the Analytics stream.
 * The analytics stream does not explicitly implement the vectoredRead() method,
 * or currently do and vectored-read specific optimisations
 * (such as range coalescing). However, this test ensures that the base
 * implementation of readVectored {@link org.apache.hadoop.fs.PositionedReadable}
 * still works.
 */
@IntegrationTest
@ParameterizedClass(name="buffer-{0}")
@MethodSource("params")
public class ITestS3AContractAnalyticsStreamVectoredRead extends AbstractContractVectoredReadTest {

  public ITestS3AContractAnalyticsStreamVectoredRead(String bufferType) {
    super(bufferType);
  }

  private static final String REQUEST_COALESCE_TOLERANCE_KEY =
          ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX + "." + AAL_REQUEST_COALESCE_TOLERANCE;

  private static final String READ_BUFFER_SIZE_KEY =
          ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX + "." + AAL_READ_BUFFER_SIZE;

  private static final String SMALL_OBJECT_PREFETCH_ENABLED_KEY =
          ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX + "." + AAL_SMALL_OBJECT_PREFETCH_ENABLED;

  private static final String CACHE_TIMEOUT_KEY =
          ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX + "." + AAL_CACHE_TIMEOUT;

  /**
   * Create a configuration.
   * @return a configuration
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();

    S3ATestUtils.disableFilesystemCaching(conf);

    removeBaseAndBucketOverrides(conf,
            REQUEST_COALESCE_TOLERANCE_KEY,
            READ_BUFFER_SIZE_KEY,
            SMALL_OBJECT_PREFETCH_ENABLED_KEY,
            CACHE_TIMEOUT_KEY);

    // Set the coalesce tolerance to 1KB, default is 1MB.
    conf.setInt(REQUEST_COALESCE_TOLERANCE_KEY, S_16K);

    // Set the minimum block size to 32KB. AAL uses a default block size of 128KB, which means the minimum size a S3
    // request will be is 128KB. Since the file being read is 128KB, we need to  use this here to demonstrate that
    // separate GET requests are made for ranges that are not coalesced.
    conf.setInt(READ_BUFFER_SIZE_KEY, S_32K);

    // Disable small object prefetched, otherwise anything less than 8MB is fetched in a single GET.
    conf.set(SMALL_OBJECT_PREFETCH_ENABLED_KEY, "false");

    conf.setInt(CACHE_TIMEOUT_KEY, 5000);

    enableAnalyticsAccelerator(conf);
    // If encryption is set, some AAL tests will fail.
    // This is because AAL caches the head request response, and uses
    // the eTag when making a GET request. When using encryption, the eTag is
    // no longer a hash of the object content, and is not always the same when
    // the same object is created multiple times. This test creates the file
    // vectored_file.txt before running each test, which will have a
    // different eTag when using encryption, leading to preconditioned failures.
    // This issue is tracked in:
    // https://github.com/awslabs/analytics-accelerator-s3/issues/218
    skipForAnyEncryptionExceptSSES3(conf);
    return conf;
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  /**
   * When the offset is negative, AAL returns IllegalArgumentException, whereas the base implementation will return
   * an EoF.
   */
  @Override
  public void testNegativeOffsetRange()  throws Exception {
    verifyExceptionalVectoredRead(ContractTestUtils.range(-1, 50), IllegalArgumentException.class);
  }

  /**
   * Currently there is no null check on the release operation, this will be fixed in the next AAL version.
   */
  @Override
  public void testNullReleaseOperation()  {
    skip("AAL current does not do a null check on the release operation");
  }

  @Test
  public void testReadVectoredWithAALStatsCollection() throws Exception {
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(FileRange.createFileRange(0, 100));
    fileRanges.add(FileRange.createFileRange(800, 200));
    fileRanges.add(FileRange.createFileRange(4 * S_1K, 4 * S_1K));
    fileRanges.add(FileRange.createFileRange(80 * S_1K, 4 * S_1K));

    try (FSDataInputStream in = openVectorFile()) {
      in.readVectored(fileRanges, getAllocate());

      validateVectoredReadResult(fileRanges, DATASET, 0);
      IOStatistics st = in.getIOStatistics();

      verifyStatisticCounterValue(st,
              StreamStatisticNames.STREAM_READ_ANALYTICS_OPENED, 1);

      verifyStatisticCounterValue(st,
              StreamStatisticNames.STREAM_READ_VECTORED_OPERATIONS,
              1);

      // Verify ranges are coalesced, we are using a coalescing tolerance of 16KB, so [0-100, 800-200, 4KB-8KB] will
      // get coalesced into a single range.
      verifyStatisticCounterValue(st, StreamStatisticNames.STREAM_READ_VECTORED_INCOMING_RANGES, 4);
      verifyStatisticCounterValue(st, StreamStatisticNames.STREAM_READ_VECTORED_COMBINED_RANGES, 2);

      verifyStatisticCounterValue(st, ACTION_HTTP_GET_REQUEST, 2);

      // read the same ranges again to demonstrate that the data is cached, and no new GETs are made.
      in.readVectored(fileRanges, getAllocate());
      verifyStatisticCounterValue(st, ACTION_HTTP_GET_REQUEST, 2);

      // Because of how AAL is currently written, it is not possible to track cache hits that originate from a
      // readVectored() accurately. For this reason, cache hits from readVectored are currently not tracked, for more
      // details see: https://github.com/awslabs/analytics-accelerator-s3/issues/359
      verifyStatisticCounterValue(st, StreamStatisticNames.STREAM_READ_CACHE_HIT, 0);
    }

  }
}
