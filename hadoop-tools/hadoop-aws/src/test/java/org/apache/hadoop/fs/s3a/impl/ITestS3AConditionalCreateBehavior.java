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

package org.apache.hadoop.fs.s3a.impl;

import java.util.Arrays;
import java.util.Collection;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3ATestUtils;

import static org.apache.hadoop.fs.Options.CreateFileOptionKeys.FS_OPTION_CREATE_CONDITIONAL_OVERWRITE;
import static org.apache.hadoop.fs.Options.CreateFileOptionKeys.FS_OPTION_CREATE_CONDITIONAL_OVERWRITE_ETAG;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CONDITIONAL_CREATE_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_PERFORMANCE;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_PERFORMANCE_FLAGS;
import static org.apache.hadoop.fs.s3a.Constants.MIN_MULTIPART_THRESHOLD;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_SIZE;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.UPLOAD_PART_COUNT_LIMIT;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assumptions.assumeThat;

@RunWith(Parameterized.class)
public class ITestS3AConditionalCreateBehavior extends AbstractS3ATestBase {

  private static final byte[] SMALL_FILE_BYTES = dataset(TEST_FILE_LEN, 0, 255);

  private final boolean conditionalCreateEnabled;

  public ITestS3AConditionalCreateBehavior(boolean conditionalCreateEnabled) {
    this.conditionalCreateEnabled = conditionalCreateEnabled;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
            {true},
            {false}
    });
  }

  /**
   * Asserts that the FSDataOutputStream has the conditional create capability enabled.
   *
   * @param stream The output stream to check.
   */
  private static void assertHasCapabilityConditionalCreate(FSDataOutputStream stream) {
    Assertions.assertThat(stream.hasCapability(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE))
            .as("Conditional create capability should be enabled")
            .isTrue();
  }

  /**
   * Asserts that the FSDataOutputStream has the ETag-based conditional create capability enabled.
   *
   * @param stream The output stream to check.
   */
  private static void assertHasCapabilityEtagWrite(FSDataOutputStream stream) {
    Assertions.assertThat(stream.hasCapability(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE_ETAG))
            .as("ETag-based conditional create capability should be enabled")
            .isTrue();
  }

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    removeBaseAndBucketOverrides(
            conf,
            FS_S3A_CREATE_PERFORMANCE,
            FS_S3A_PERFORMANCE_FLAGS,
            MULTIPART_SIZE,
            MIN_MULTIPART_THRESHOLD,
            UPLOAD_PART_COUNT_LIMIT
    );
    if (!conditionalCreateEnabled) {
      conf.setBoolean(FS_S3A_CONDITIONAL_CREATE_ENABLED, false);
    }
    S3ATestUtils.disableFilesystemCaching(conf);
    return conf;
  }

  @Before
  public void setUp() throws Exception {
    super.setup();
  }

  @Test
  public void testConditionalWrite() throws Throwable {
    FileSystem fs = getFileSystem();
    Path testFile = methodPath();
    fs.mkdirs(testFile.getParent());

    // create a file over an empty path
    try (FSDataOutputStream stream = fs.create(testFile)) {
      stream.write(SMALL_FILE_BYTES);
    }

    // attempted conditional overwrite fails
    intercept(PathIOException.class, () -> {
      FSDataOutputStreamBuilder cf = fs.createFile(testFile);
      cf.opt(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE, true);
      try (FSDataOutputStream stream = cf.build()) {
        assertHasCapabilityConditionalCreate(stream);
        stream.write(SMALL_FILE_BYTES);
      }
    });
  }

  @Test
  public void testWriteWithEtag() throws Throwable {
    assumeThat(conditionalCreateEnabled)
            .as("Skipping as conditional create is enabled")
            .isFalse();

    FileSystem fs = getFileSystem();
    Path testFile = methodPath();
    fs.mkdirs(testFile.getParent());

    // create a file over an empty path
    try (FSDataOutputStream stream = fs.create(testFile)) {
      stream.write(SMALL_FILE_BYTES);
    }

    String etag = ((S3AFileStatus) fs.getFileStatus(testFile)).getEtag();
    Assertions.assertThat(etag)
            .as("ETag should not be null after file creation")
            .isNotNull();

    // attempted write with etag. should fail
    intercept(PathIOException.class, () -> {
      FSDataOutputStreamBuilder cf = fs.createFile(testFile);
      cf.must(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE_ETAG, etag);
      try (FSDataOutputStream stream = cf.build()) {
        assertHasCapabilityEtagWrite(stream);
        stream.write(SMALL_FILE_BYTES);
      }
    });
  }

  @Test
  public void testWriteWithPerformanceFlagAndOverwriteFalse() throws Throwable {
    assumeThat(conditionalCreateEnabled)
            .as("Skipping as conditional create is enabled")
            .isFalse();

    FileSystem fs = getFileSystem();
    Path testFile = methodPath();
    fs.mkdirs(testFile.getParent());

    // create a file over an empty path
    try (FSDataOutputStream stream = fs.create(testFile)) {
      stream.write(SMALL_FILE_BYTES);
    }

    // overwrite with performance flag
    FSDataOutputStreamBuilder cf = fs.createFile(testFile);
    cf.overwrite(false);
    cf.must(FS_S3A_CREATE_PERFORMANCE, true);
    IOStatistics ioStatistics;
    try (FSDataOutputStream stream = cf.build()) {
      stream.write(SMALL_FILE_BYTES);
      ioStatistics = S3ATestUtils.getOutputStreamStatistics(stream).getIOStatistics();
    }
    // TODO: uncomment when statistics are getting initialised
    // verifyStatisticCounterValue(ioStatistics, Statistic.CONDITIONAL_CREATE.getSymbol(), 0);
    // verifyStatisticCounterValue(ioStatistics, Statistic.CONDITIONAL_CREATE_FAILED.getSymbol(), 0);
  }
}
