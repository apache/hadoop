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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.tags.IntegrationTest;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.fs.s3a.Constants.DELETE_NON_EMPTY_DIRECTORY_ENABLED;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;

/**
 * Test that fs.s3a.delete.non-empty-directory.enabled is reflected in
 * hasPathCapability(DELETE_NON_EMPTY_DIRECTORY_ENABLED). HADOOP-19801.
 */
@IntegrationTest
public class ITestS3ADeleteNonEmptyDirectoryCapability extends
    AbstractS3ATestBase {

  /**
   * Test that when the option is disabled (default), the capability is false.
   */
  @Test
  public void testCapabilityDisabledByDefault() throws Throwable {
    try (S3AFileSystem fs = createCapabilityTestFileSystem(false)) {
      Assertions.assertThat(fs.hasPathCapability(new Path("/"),
          DELETE_NON_EMPTY_DIRECTORY_ENABLED))
          .describedAs("path capability when option not set")
          .isFalse();
    }
  }

  /**
   * Test that when the option is enabled, the capability is true.
   * Creates a new S3AFileSystem with the option set and verifies the
   * capability.
   */
  @Test
  public void testCapabilityWhenEnabled() throws Throwable {
    try (S3AFileSystem fs = createCapabilityTestFileSystem(true)) {
      Assertions.assertThat(fs.hasPathCapability(new Path("/"),
          DELETE_NON_EMPTY_DIRECTORY_ENABLED))
          .describedAs("path capability when option enabled")
          .isTrue();
    }
  }

  private S3AFileSystem createCapabilityTestFileSystem(
      final boolean enabled) throws Exception {
    Configuration conf = new Configuration(getFileSystem().getConf());
    removeBaseAndBucketOverrides(getTestBucketName(conf), conf,
        DELETE_NON_EMPTY_DIRECTORY_ENABLED);
    disableFilesystemCaching(conf);
    conf.setBoolean(DELETE_NON_EMPTY_DIRECTORY_ENABLED, enabled);
    S3AFileSystem fs = new S3AFileSystem();
    fs.initialize(getFileSystem().getUri(), conf);
    return fs;
  }
}
