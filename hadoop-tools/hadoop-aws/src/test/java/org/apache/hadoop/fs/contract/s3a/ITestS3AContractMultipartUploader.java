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

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.MultipartUploader;
import org.apache.hadoop.fs.PartHandle;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UploadHandle;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractContractMultipartUploaderTest;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.scale.AbstractSTestS3AHugeFiles.DEFAULT_HUGE_PARTITION_SIZE;
import static org.apache.hadoop.test.LambdaTestUtils.eventually;

/**
 * Test MultipartUploader with S3A.
 * Although not an S3A Scale test subclass, it uses the -Dscale option
 * to enable it, and partition size option to control the size of
 * parts uploaded.
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
   * Extend superclass teardown with actions to help clean up the S3 store,
   * including aborting uploads under the test path.
   */
  @Override
  public void teardown() throws Exception {
    Path teardown = path("teardown").getParent();
    S3AFileSystem fs = getFileSystem();
    if (fs != null) {
      WriteOperationHelper helper = fs.getWriteOperationHelper();
      try {
        LOG.info("Teardown: aborting outstanding uploads under {}", teardown);
        int count = helper.abortMultipartUploadsUnderPath(
            fs.pathToKey(teardown));
        LOG.info("Found {} incomplete uploads", count);
      } catch (Exception e) {
        LOG.warn("Exeception in teardown", e);
      }
    }
    super.teardown();
  }

  /**
   * S3 has no concept of directories, so this test does not apply.
   */
  public void testDirectoryInTheWay() throws Exception {
    // no-op
  }

  @Override
  public void testMultipartUploadReverseOrder() throws Exception {
    ContractTestUtils.skip("skipped for speed");
  }

  /**
   * This creates and then deletes a zero-byte file while an upload
   * is in progress, and verifies that the uploaded file is ultimately
   * visible.
   */
  @Test
  public void testMultipartOverlapWithTransientFile() throws Throwable {
    // until there's a way to explicitly ask for a multipart uploader from a
    // specific FS, explicitly create one bonded to the raw FS.
    describe("testMultipartOverlapWithTransientFile");
    S3AFileSystem fs = getFileSystem();
    Path path = path("testMultipartOverlapWithTransientFile");
    fs.delete(path, true);
    MultipartUploader mpu = mpu(1);
    UploadHandle upload1 = mpu.initialize(path);
    byte[] dataset = dataset(1024, '0', 10);
    final Map<Integer, PartHandle> handles = new HashMap<>();
    LOG.info("Uploading multipart entry");
    PartHandle value = mpu.putPart(path, new ByteArrayInputStream(dataset), 1,
        upload1,
        dataset.length);
    // upload 1K
    handles.put(1, value);
    // confirm the path is absent
    ContractTestUtils.assertPathDoesNotExist(fs,
        "path being uploaded", path);
    // now create an empty file
    ContractTestUtils.touch(fs, path);
    final FileStatus touchStatus = fs.getFileStatus(path);
    LOG.info("0-byte file has been created: {}", touchStatus);
    fs.delete(path, false);
    // now complete the upload
    mpu.complete(path, handles, upload1);

    // wait for the data to arrive
    eventually(timeToBecomeConsistentMillis(), 500, () -> {
      FileStatus mpuStatus = fs.getFileStatus(path);
      assertTrue("File is empty in " + mpuStatus, mpuStatus.getLen() > 0);
      return mpuStatus;
    });

  }
}
