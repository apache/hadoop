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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractContractMultipartUploaderTest;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;

import static org.apache.hadoop.fs.s3a.S3ATestConstants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.scale.AbstractSTestS3AHugeFiles.DEFAULT_HUGE_PARTITION_SIZE;

/**
 * Test MultipartUploader with S3A.
 */
public class ITestS3AContractMultipartUploader extends
    AbstractContractMultipartUploaderTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AContractMultipartUploader.class);

  private int partitionSize;

  /**
   * S3 requires a minimum part size of 5MB (except the last part).
   * @return 5MB
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
   * Extend superclass teardown with actions to help clean up the S3 store,
   * including aborting uploads under the test path.
   */
  @Override
  public void teardown() throws Exception {
    Path teardown = path("teardown").getParent();
    S3AFileSystem fs = getFileSystem();
    WriteOperationHelper helper = fs.getWriteOperationHelper();
    try {
      LOG.info("Teardown: aborting outstanding uploads under {}", teardown);
      int count = helper.abortMultipartUploadsUnderPath(fs.pathToKey(teardown));
      LOG.info("Found {} incomplete uploads", count);
    } catch (IOException e) {
      LOG.warn("IOE in teardown", e);
    }
    super.teardown();
  }

  @Override
  public void testMultipartUploadEmptyPart() throws Exception {
    // ignore the test in the base class.
  }
}
