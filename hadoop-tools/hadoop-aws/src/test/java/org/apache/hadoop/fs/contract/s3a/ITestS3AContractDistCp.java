/**
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

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.SCALE_TEST_TIMEOUT_MILLIS;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.maybeEnableS3Guard;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.s3a.FailureInjectionPolicy;
import org.apache.hadoop.tools.contract.AbstractContractDistCpTest;

/**
 * Contract test suite covering S3A integration with DistCp.
 * Uses the block output stream, buffered to disk. This is the
 * recommended output mechanism for DistCP due to its scalability.
 */
public class ITestS3AContractDistCp extends AbstractContractDistCpTest {

  private static final long MULTIPART_SETTING = MULTIPART_MIN_SIZE;

  @Override
  protected int getTestTimeoutMillis() {
    return SCALE_TEST_TIMEOUT_MILLIS;
  }

  /**
   * Create a configuration, possibly patching in S3Guard options.
   * @return a configuration
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration newConf = super.createConfiguration();
    newConf.setLong(MULTIPART_SIZE, MULTIPART_SETTING);
    newConf.set(FAST_UPLOAD_BUFFER, FAST_UPLOAD_BUFFER_DISK);
    // patch in S3Guard options
    maybeEnableS3Guard(newConf);
    return newConf;
  }

  @Override
  protected S3AContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  /**
   * Always inject the delay path in, so if the destination is inconsistent,
   * and uses this key, inconsistency triggered.
   * @param filepath path string in
   * @return path on the remote FS for distcp
   * @throws IOException IO failure
   */
  @Override
  protected Path path(final String filepath) throws IOException {
    Path path = super.path(filepath);
    return new Path(path, FailureInjectionPolicy.DEFAULT_DELAY_KEY_SUBSTRING);
  }

  @Override
  public void testDirectWrite() throws Exception {
    resetStorageStatistics();
    super.testDirectWrite();
    assertEquals("Expected no renames for a direct write distcp", 0L,
        getRenameOperationCount());
  }

  @Override
  public void testNonDirectWrite() throws Exception {
    resetStorageStatistics();
    try {
      super.testNonDirectWrite();
    } catch (FileNotFoundException e) {
      // We may get this exception when data is written to a DELAY_LISTING_ME
      // directory causing verification of the distcp success to fail if
      // S3Guard is not enabled
    }
    assertEquals("Expected 2 renames for a non-direct write distcp", 2L,
        getRenameOperationCount());
  }

  private void resetStorageStatistics() {
    getFileSystem().getStorageStatistics().reset();
  }

  private long getRenameOperationCount() {
    return getFileSystem().getStorageStatistics()
        .getLong(StorageStatistics.CommonStatisticNames.OP_RENAME);
  }
}
