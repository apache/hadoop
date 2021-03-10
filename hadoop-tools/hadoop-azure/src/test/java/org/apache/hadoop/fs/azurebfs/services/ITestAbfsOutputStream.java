/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;

/**
 * Test create operation.
 */
public class ITestAbfsOutputStream extends AbstractAbfsIntegrationTest {
  private static final Path TEST_FILE_PATH = new Path("testfile");

  public ITestAbfsOutputStream() throws Exception {
    super();
  }

  @Test
  public void testMaxRequestsAndQueueCapacityDefaults() throws Exception {
    Configuration conf = getRawConfiguration();
    final AzureBlobFileSystem fs = getFileSystem(conf);
    try (FSDataOutputStream out = fs.create(TEST_FILE_PATH)) {
    AbfsOutputStream stream = (AbfsOutputStream) out.getWrappedStream();

      int maxConcurrentRequests
          = getConfiguration().getWriteMaxConcurrentRequestCount();
      if (stream.isAppendBlobStream()) {
        maxConcurrentRequests = 1;
      }

    Assertions.assertThat(stream.getMaxConcurrentRequestCount()).describedAs(
        "maxConcurrentRequests should be " + maxConcurrentRequests)
        .isEqualTo(maxConcurrentRequests);
    Assertions.assertThat(stream.getMaxRequestsThatCanBeQueued()).describedAs(
        "maxRequestsToQueue should be " + getConfiguration()
            .getMaxWriteRequestsToQueue())
        .isEqualTo(getConfiguration().getMaxWriteRequestsToQueue());
    }
  }

  @Test
  public void testMaxRequestsAndQueueCapacity() throws Exception {
    Configuration conf = getRawConfiguration();
    int maxConcurrentRequests = 6;
    int maxRequestsToQueue = 10;
    conf.set(ConfigurationKeys.AZURE_WRITE_MAX_CONCURRENT_REQUESTS,
        "" + maxConcurrentRequests);
    conf.set(ConfigurationKeys.AZURE_WRITE_MAX_REQUESTS_TO_QUEUE,
        "" + maxRequestsToQueue);
    final AzureBlobFileSystem fs = getFileSystem(conf);
    FSDataOutputStream out = fs.create(TEST_FILE_PATH);
    AbfsOutputStream stream = (AbfsOutputStream) out.getWrappedStream();

    if (stream.isAppendBlobStream()) {
      maxConcurrentRequests = 1;
    }

    Assertions.assertThat(stream.getMaxConcurrentRequestCount())
        .describedAs("maxConcurrentRequests should be " + maxConcurrentRequests)
        .isEqualTo(maxConcurrentRequests);
    Assertions.assertThat(stream.getMaxRequestsThatCanBeQueued())
        .describedAs("maxRequestsToQueue should be " + maxRequestsToQueue)
        .isEqualTo(maxRequestsToQueue);
  }

}
