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

package org.apache.hadoop.fs.azurebfs.services;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_TAIL_LATENCY_REQUEST_TIMEOUT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_TAIL_LATENCY_TRACKER;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_NETWORKING_LIBRARY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for AbfsTailLatencyTracker related configurations.
 */
public class ITestAbfsTailLatencyTracker extends AbstractAbfsIntegrationTest {

  protected ITestAbfsTailLatencyTracker() throws Exception {
  }

  /**
   * Test to verify that tail latency timeout is enabled and disabled with all conditions.
   * @throws Exception
   */
  @Test
  public void testTailLatencyTimeoutEnabled() throws Exception {
    Configuration conf = new Configuration(getRawConfiguration());
    conf.set(FS_AZURE_ENABLE_TAIL_LATENCY_TRACKER, "true");
    conf.set(FS_AZURE_NETWORKING_LIBRARY, "APACHE_HTTP_CLIENT");
    conf.set(FS_AZURE_ENABLE_TAIL_LATENCY_REQUEST_TIMEOUT, "true");

    // All conditions met for enabling timeout
    assertTailLatencyTimeoutEnabled(conf, true);

    // Verify that disabling timeout alone disabled timeout
    conf.set(FS_AZURE_ENABLE_TAIL_LATENCY_REQUEST_TIMEOUT, "false");
    assertTailLatencyTimeoutEnabled(conf, false);

    // Verify that disabling tracker alone disabled timeout
    conf.set(FS_AZURE_ENABLE_TAIL_LATENCY_TRACKER, "false");
    conf.set(FS_AZURE_ENABLE_TAIL_LATENCY_REQUEST_TIMEOUT, "true");
    assertTailLatencyTimeoutEnabled(conf, false);

    // Verify that enabling both but using JDK networking library disabled timeout
    conf.set(FS_AZURE_ENABLE_TAIL_LATENCY_TRACKER, "true");
    conf.set(FS_AZURE_NETWORKING_LIBRARY, "JDK_HTTP_URL_CONNECTION");
    conf.set(FS_AZURE_ENABLE_TAIL_LATENCY_REQUEST_TIMEOUT, "true");
    assertTailLatencyTimeoutEnabled(conf, false);
  }

  private void assertTailLatencyTimeoutEnabled(Configuration conf, boolean expected) throws Exception {
    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.get(conf)) {
      Path testPath = new Path("/testFile");
      fs.create(testPath).close();
      AbfsClient client = fs.getAbfsStore().getClient();
      AbfsRestOperation op = client.getPathStatus("/testFile", false,
          getTestTracingContext(fs, false), null);
      assertThat(op.isTailLatencyTimeoutEnabled()).isEqualTo(expected);
    }
  }
}
