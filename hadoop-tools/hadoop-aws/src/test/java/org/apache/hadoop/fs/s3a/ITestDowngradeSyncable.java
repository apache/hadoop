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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;
import org.apache.hadoop.fs.statistics.IOStatistics;

import static org.apache.hadoop.fs.s3a.Constants.DOWNGRADE_SYNCABLE_EXCEPTIONS;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBucketOverrides;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticCounter;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToString;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_HFLUSH;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_HSYNC;


public class ITestDowngradeSyncable extends AbstractS3ACostTest {

  protected static final Logger LOG =
      LoggerFactory.getLogger(ITestDowngradeSyncable.class);


  public ITestDowngradeSyncable() {
    super(false, true, false);
  }

  @Override
  public Configuration createConfiguration() {
    final Configuration conf = super.createConfiguration();
    String bucketName = getTestBucketName(conf);
    removeBucketOverrides(bucketName, conf,
        DOWNGRADE_SYNCABLE_EXCEPTIONS);
    conf.setBoolean(DOWNGRADE_SYNCABLE_EXCEPTIONS, true);
    return conf;
  }

  @Test
  public void testHFlushDowngrade() throws Throwable {
    describe("Verify that hflush() calls can be downgraded from fail"
        + " to ignore; the relevant counter is updated");
    Path path = methodPath();
    S3AFileSystem fs = getFileSystem();
    final IOStatistics fsIoStats = fs.getIOStatistics();
    assertThatStatisticCounter(fsIoStats, OP_HFLUSH)
        .isEqualTo(0);

    try (FSDataOutputStream out = fs.create(path, true)) {
      out.write('1');
      // must succeed
      out.hflush();
      // stats counter records the downgrade
      IOStatistics iostats = out.getIOStatistics();
      LOG.info("IOStats {}", ioStatisticsToString(iostats));
      assertThatStatisticCounter(iostats, OP_HFLUSH)
          .isEqualTo(1);
      assertThatStatisticCounter(iostats, OP_HSYNC)
          .isEqualTo(0);
    }
    // once closed. the FS will have its stats merged.
    assertThatStatisticCounter(fsIoStats, OP_HFLUSH)
        .isEqualTo(1);
  }

  @Test
  public void testHSyncDowngrade() throws Throwable {
    describe("Verify that hsync() calls can be downgraded from fail"
        + " to ignore; the relevant counter is updated");
    Path path = methodPath();
    S3AFileSystem fs = getFileSystem();
    final IOStatistics fsIoStats = fs.getIOStatistics();
    assertThatStatisticCounter(fsIoStats, OP_HSYNC)
        .isEqualTo(0);

    try (FSDataOutputStream out = fs.create(path, true)) {
      out.write('1');
      // must succeed
      out.hsync();
      // stats counter records the downgrade
      IOStatistics iostats = out.getIOStatistics();
      LOG.info("IOStats {}", ioStatisticsToString(iostats));
      assertThatStatisticCounter(iostats, OP_HFLUSH)
          .isEqualTo(0);
      assertThatStatisticCounter(iostats, OP_HSYNC)
          .isEqualTo(1);
    }
    // once closed. the FS will have its stats merged.
    assertThatStatisticCounter(fsIoStats, OP_HSYNC)
        .isEqualTo(1);
  }

}
