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

package org.apache.hadoop.fs.s3a.statistics;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;

import static org.apache.hadoop.fs.s3a.Constants.S3EXPRESS_CREATE_SESSION;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.setPerformanceFlags;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_IO_REQUEST;

/**
 * Verify that AWS SDK statistics are wired up.
 */
public class ITestAWSStatisticCollection extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAWSStatisticCollection.class);

  @Override
  public Configuration createConfiguration() {
    final Configuration conf = super.createConfiguration();
    S3ATestUtils.removeBaseAndBucketOverrides(conf,
        S3EXPRESS_CREATE_SESSION);
    setPerformanceFlags(conf, "create");
    conf.setBoolean(S3EXPRESS_CREATE_SESSION, false);
    return conf;
  }

  @Test
  public void testSDKMetricsCostOfGetFileStatusOnFile() throws Throwable {
    describe("Performing getFileStatus() on a file");
    Path simpleFile = file(methodPath());
    // and repeat on the file looking at AWS wired up stats
    final S3AFileSystem fs = getFileSystem();
    LOG.info("Initiating GET request for {}", simpleFile);
    verifyMetrics(() -> fs.getFileStatus(simpleFile),
        with(STORE_IO_REQUEST, 1));
  }

}
