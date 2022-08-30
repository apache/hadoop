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

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.SCALE_TEST_TIMEOUT_MILLIS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageStatistics;
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
   * Create a configuration.
   * @return a configuration
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration newConf = super.createConfiguration();
    newConf.setLong(MULTIPART_SIZE, MULTIPART_SETTING);
    newConf.set(FAST_UPLOAD_BUFFER, FAST_UPLOAD_BUFFER_DISK);
    return newConf;
  }

  @Override
  protected boolean shouldUseDirectWrite() {
    return true;
  }

  @Override
  protected S3AContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  @Override
  public void testDistCpWithIterator() throws Exception {
    final long renames = getRenameOperationCount();
    super.testDistCpWithIterator();
    assertEquals("Expected no renames for a direct write distcp",
        getRenameOperationCount(),
         renames);
  }

  @Override
  public void testNonDirectWrite() throws Exception {
    final long renames = getRenameOperationCount();
    super.testNonDirectWrite();
    assertEquals("Expected 2 renames for a non-direct write distcp", 2L,
        getRenameOperationCount() - renames);
  }

  private long getRenameOperationCount() {
    return getFileSystem().getStorageStatistics()
        .getLong(StorageStatistics.CommonStatisticNames.OP_RENAME);
  }
}
