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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.contract.AbstractContractVectoredReadTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;
import org.apache.hadoop.test.tags.IntegrationTest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.contract.ContractTestUtils.validateVectoredReadResult;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.enableAnalyticsAccelerator;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipForAnyEncryptionExceptSSES3;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;

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

  /**
   * Create a configuration.
   * @return a configuration
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
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

    List<FileRange> fileRanges = createSampleNonOverlappingRanges();
    try (FSDataInputStream in = openVectorFile()) {
      in.readVectored(fileRanges, getAllocate());

      validateVectoredReadResult(fileRanges, DATASET, 0);
      IOStatistics st = in.getIOStatistics();

      // Statistics such as GET requests will be added after IoStats support.
      verifyStatisticCounterValue(st,
              StreamStatisticNames.STREAM_READ_ANALYTICS_OPENED, 1);

      verifyStatisticCounterValue(st,
              StreamStatisticNames.STREAM_READ_VECTORED_OPERATIONS,
              1);
    }
  }
}
