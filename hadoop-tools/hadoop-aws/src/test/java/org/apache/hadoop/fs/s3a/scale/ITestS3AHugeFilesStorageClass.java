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

package org.apache.hadoop.fs.s3a.scale;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.contract.ContractTestUtils.bandwidth;
import static org.apache.hadoop.fs.contract.ContractTestUtils.toHuman;
import static org.apache.hadoop.fs.s3a.Constants.STORAGE_CLASS;
import static org.apache.hadoop.fs.s3a.Constants.STORAGE_CLASS_REDUCED_REDUNDANCY;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfStorageClassTestsDisabled;

/**
 * Class to verify that {@link Constants#STORAGE_CLASS} is set correctly
 * for creating and renaming huge files with multipart upload requests.
 */
public class ITestS3AHugeFilesStorageClass extends AbstractSTestS3AHugeFiles {

  private static final Logger LOG = LoggerFactory.getLogger(ITestS3AHugeFilesStorageClass.class);

  @Override
  protected Configuration createScaleConfiguration() {
    Configuration conf = super.createScaleConfiguration();
    skipIfStorageClassTestsDisabled(conf);
    disableFilesystemCaching(conf);
    removeBaseAndBucketOverrides(conf, STORAGE_CLASS);

    conf.set(STORAGE_CLASS, STORAGE_CLASS_REDUCED_REDUNDANCY);
    return conf;
  }

  @Override
  protected String getBlockOutputBufferName() {
    return Constants.FAST_UPLOAD_BUFFER_ARRAY;
  }

  @Override
  public void test_010_CreateHugeFile() throws IOException {
    super.test_010_CreateHugeFile();
    assertStorageClass(getPathOfFileToCreate());
  }

  @Override
  public void test_030_postCreationAssertions() throws Throwable {
    super.test_030_postCreationAssertions();
    assertStorageClass(getPathOfFileToCreate());
  }

  @Override
  public void test_040_PositionedReadHugeFile() throws Throwable {
    skipQuietly("PositionedReadHugeFile");
  }

  @Override
  public void test_050_readHugeFile() throws Throwable {
    skipQuietly("readHugeFile");
  }

  @Override
  public void test_090_verifyRenameSourceEncryption() throws IOException {
    skipQuietly("verifyRenameSourceEncryption");
  }

  @Override
  public void test_100_renameHugeFile() throws Throwable {
    Path hugefile = getHugefile();
    Path hugefileRenamed = getHugefileRenamed();
    assumeHugeFileExists();
    describe("renaming %s to %s", hugefile, hugefileRenamed);
    S3AFileSystem fs = getFileSystem();
    FileStatus status = fs.getFileStatus(hugefile);
    long size = status.getLen();
    fs.delete(hugefileRenamed, false);
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    fs.rename(hugefile, hugefileRenamed);
    long mb = Math.max(size / _1MB, 1);
    timer.end("time to rename file of %d MB", mb);
    LOG.info("Time per MB to rename = {} nS", toHuman(timer.nanosPerOperation(mb)));
    bandwidth(timer, size);
    FileStatus destFileStatus = fs.getFileStatus(hugefileRenamed);
    assertEquals(size, destFileStatus.getLen());
    assertStorageClass(hugefileRenamed);
  }

  @Override
  public void test_110_verifyRenameDestEncryption() throws IOException {
    skipQuietly("verifyRenameDestEncryption");
  }

  private void skipQuietly(String text) {
    describe("Skipping: %s", text);
  }

  protected void assertStorageClass(Path hugeFile) throws IOException {

    String actual = getS3AInternals().getObjectMetadata(hugeFile).storageClassAsString();

    assertTrue(
        "Storage class of object is " + actual + ", expected " + STORAGE_CLASS_REDUCED_REDUNDANCY,
        STORAGE_CLASS_REDUCED_REDUNDANCY.equalsIgnoreCase(actual));
  }
}
