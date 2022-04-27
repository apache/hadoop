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

import java.nio.file.AccessDeniedException;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticAssertions;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;

import static org.apache.hadoop.fs.s3a.Constants.ALLOW_REQUESTER_PAYS;
import static org.apache.hadoop.fs.s3a.Constants.S3A_BUCKET_PROBE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Tests for Requester Pays feature.
 */
public class ITestS3ARequesterPays extends AbstractS3ATestBase {

  @Test
  public void testRequesterPaysOptionSuccess() throws Throwable {
    describe("Test requester pays enabled case by reading last then first byte");

    Configuration conf = this.createConfiguration();
    updateConf(conf, true);
    Path requesterPaysPath = getRequesterPaysPath(conf);

    try (
        FileSystem fs = requesterPaysPath.getFileSystem(conf);
        FSDataInputStream inputStream = fs.open(requesterPaysPath);
    ) {
      long fileLength = fs.getFileStatus(requesterPaysPath).getLen();

      inputStream.seek(fileLength - 1);
      inputStream.readByte();

      // Jump back to the start, triggering a new GetObject request.
      inputStream.seek(0);
      inputStream.readByte();

      // Verify > 1 call was made, so we're sure it is correctly configured for each request
      IOStatisticAssertions
          .assertThatStatisticCounter(inputStream.getIOStatistics(),
              StreamStatisticNames.STREAM_READ_OPENED)
          .isGreaterThan(1);

      // Check list calls work without error
      fs.listFiles(requesterPaysPath.getParent(), false);
    }
  }

  @Test
  public void testRequesterPaysDisabledFails() throws Throwable {
    describe("Verify expected failure for requester pays buckets when client has it disabled");

    Configuration conf = this.createConfiguration();
    updateConf(conf, false);
    Path requesterPaysPath = getRequesterPaysPath(conf);

    try (FileSystem fs = requesterPaysPath.getFileSystem(conf)) {
      intercept(
          AccessDeniedException.class,
          "403 Forbidden",
          "Expected requester pays bucket to fail without header set",
          () -> fs.open(requesterPaysPath).close()
      );
    }
  }

  /**
   * Use this after creating the file system, as this is when bucket-specific
   * overrides are applied.
   * @param conf Hadoop configuration from FS to mutate
   * @param requesterPaysEnabled Indicate if requester pays be on or off
   */
  private static void updateConf(
      Configuration conf,
      boolean requesterPaysEnabled
  ) {
    Path requesterPaysPath = getRequesterPaysPath(conf);
    String requesterPaysBucketName = requesterPaysPath.toUri().getHost();
    S3ATestUtils.removeBaseAndBucketOverrides(
        requesterPaysBucketName,
        conf,
        ALLOW_REQUESTER_PAYS,
        S3A_BUCKET_PROBE);
    conf.setBoolean(ALLOW_REQUESTER_PAYS, requesterPaysEnabled);
    // Enable bucket exists check, the first failure point people may encounter
    conf.setInt(S3A_BUCKET_PROBE, 2);
  }

  private static Path getRequesterPaysPath(Configuration conf) {
    return new Path(PublicDatasetTestUtils.getRequesterPaysObject(conf));
  }

}
