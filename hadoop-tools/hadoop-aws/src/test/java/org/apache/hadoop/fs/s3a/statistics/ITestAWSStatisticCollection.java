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

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.statistics.IOStatistics;

import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getLandsatCSVPath;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_IO_REQUEST;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticCounter;

/**
 * Verify that AWS SDK statistics are wired up.
 * This test tries to read data from US-east-1 and us-west-2 buckets
 * so as to be confident that the nuances of region mapping
 * are handed correctly (HADOOP-13551).
 * The statistics are probed to verify that the wiring up is complete.
 */
public class ITestAWSStatisticCollection extends AbstractS3ATestBase {

  private static final Path COMMON_CRAWL_PATH
      = new Path("s3a://osm-pds/planet/planet-latest.orc");

  @Test
  public void testLandsatStatistics() throws Throwable {
    final Configuration conf = getConfiguration();
    // skips the tests if the landsat path isn't the default.
    Path path = getLandsatCSVPath(conf);
    conf.set(ENDPOINT, DEFAULT_ENDPOINT);
    conf.unset("fs.s3a.bucket.landsat-pds.endpoint");

    try (S3AFileSystem fs = (S3AFileSystem) path.getFileSystem(conf)) {
      fs.getObjectMetadata(path);
      IOStatistics iostats = fs.getIOStatistics();
      assertThatStatisticCounter(iostats,
          STORE_IO_REQUEST.getSymbol())
          .isGreaterThanOrEqualTo(1);
    }
  }

  @Test
  public void testCommonCrawlStatistics() throws Throwable {
    final Configuration conf = getConfiguration();
    // skips the tests if the landsat path isn't the default.
    getLandsatCSVPath(conf);

    Path path = COMMON_CRAWL_PATH;
    conf.set(ENDPOINT, DEFAULT_ENDPOINT);

    try (S3AFileSystem fs = (S3AFileSystem) path.getFileSystem(conf)) {
      fs.getObjectMetadata(path);
      IOStatistics iostats = fs.getIOStatistics();
      assertThatStatisticCounter(iostats,
          STORE_IO_REQUEST.getSymbol())
          .isGreaterThanOrEqualTo(1);
    }
  }

}
