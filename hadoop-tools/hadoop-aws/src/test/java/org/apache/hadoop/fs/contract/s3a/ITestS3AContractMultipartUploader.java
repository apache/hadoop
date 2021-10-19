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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractMultipartUploaderTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.DEFAULT_SCALE_TESTS_ENABLED;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.KEY_HUGE_PARTITION_SIZE;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.KEY_SCALE_TESTS_ENABLED;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.SCALE_TEST_TIMEOUT_MILLIS;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestPropertyBool;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestPropertyBytes;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.maybeEnableS3Guard;
import static org.apache.hadoop.fs.s3a.scale.AbstractSTestS3AHugeFiles.DEFAULT_HUGE_PARTITION_SIZE;

/**
 * Test MultipartUploader with S3A.
 * <p></p>
 * Although not an S3A Scale test subclass, it uses the -Dscale option
 * to enable it, and partition size option to control the size of
 * parts uploaded.
 */
public class ITestS3AContractMultipartUploader extends
    AbstractContractMultipartUploaderTest {

  private int partitionSize;

  /**
   * S3 requires a minimum part size of 5MB (except the last part).
   * @return 5MB+ value
   */
  @Override
  protected int partSizeInBytes() {
    return partitionSize;
  }

  @Override
  protected int getTestPayloadCount() {
    return 3;
  }

  @Override
  public S3AFileSystem getFileSystem() {
    return (S3AFileSystem) super.getFileSystem();
  }

  /**
   * Create a configuration, possibly patching in S3Guard options.
   * @return a configuration
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    maybeEnableS3Guard(conf);
    return conf;
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  /**
   * Bigger test: use the scale timeout.
   * @return the timeout for scale tests.
   */
  @Override
  protected int getTestTimeoutMillis() {
    return SCALE_TEST_TIMEOUT_MILLIS;
  }


  @Override
  protected boolean supportsConcurrentUploadsToSamePath() {
    return true;
  }

  /**
   * Provide a pessimistic time to become consistent.
   * @return a time in milliseconds
   */
  @Override
  protected int timeToBecomeConsistentMillis() {
    return 30 * 1000;
  }

  @Override
  protected boolean finalizeConsumesUploadIdImmediately() {
    return false;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    Configuration conf = getContract().getConf();
    boolean enabled = getTestPropertyBool(
        conf,
        KEY_SCALE_TESTS_ENABLED,
        DEFAULT_SCALE_TESTS_ENABLED);
    assume("Scale test disabled: to enable set property " +
            KEY_SCALE_TESTS_ENABLED,
        enabled);
    partitionSize = (int) getTestPropertyBytes(conf,
        KEY_HUGE_PARTITION_SIZE,
        DEFAULT_HUGE_PARTITION_SIZE);
  }

  /**
   * S3 has no concept of directories, so this test does not apply.
   */
  public void testDirectoryInTheWay() throws Exception {
    skip("unsupported");
  }

  @Override
  public void testMultipartUploadReverseOrder() throws Exception {
    skip("skipped for speed");
  }
}
