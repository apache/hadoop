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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.utils.MetricFormat;
import org.junit.Test;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_DELETE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_METRIC_ACCOUNT_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_METRIC_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_METRIC_FORMAT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_METRIC_URI;
import static org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType.DeletePath;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.Assert;
import java.net.HttpURLConnection;

public class TestAbfsRestOperation extends
    AbstractAbfsIntegrationTest {

  public TestAbfsRestOperation() throws Exception {
  }

  private void checkPrerequisites() {
    assumeValidTestConfigPresent(getRawConfiguration(), FS_AZURE_METRIC_ACCOUNT_NAME);
    assumeValidTestConfigPresent(getRawConfiguration(), FS_AZURE_METRIC_ACCOUNT_KEY);
    assumeValidTestConfigPresent(getRawConfiguration(), FS_AZURE_METRIC_URI);
  }

  /**
   * Test for backoff retry metrics.
   *
   * This method tests backoff retry metrics by creating an AzureBlobFileSystem, initializing an
   * AbfsClient, and performing mock operations on an AbfsRestOperation. The method then updates
   * backoff metrics using the AbfsRestOperation.
   *
   */
  @Test
  public void testBackoffRetryMetrics() throws Exception {
    checkPrerequisites();
    // Create an AzureBlobFileSystem instance.
    final Configuration configuration = getRawConfiguration();
    configuration.set(FS_AZURE_METRIC_FORMAT, String.valueOf(MetricFormat.INTERNAL_BACKOFF_METRIC_FORMAT));
    final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    AbfsConfiguration abfsConfiguration = fs.getAbfsStore().getAbfsConfiguration();

    // Get an instance of AbfsClient and AbfsRestOperation.
    AbfsClient testClient = super.getAbfsClient(super.getAbfsStore(fs));
    AbfsRestOperation op = ITestAbfsClient.getRestOp(
        DeletePath, testClient, HTTP_METHOD_DELETE,
        ITestAbfsClient.getTestUrl(testClient, "/NonExistingPath"),
        ITestAbfsClient.getTestRequestHeaders(testClient), getConfiguration());

    // Mock retry counts and status code.
    ArrayList<String> retryCounts = new ArrayList<>(Arrays.asList("35", "28", "31", "45", "10", "2", "9"));
    int statusCode = HttpURLConnection.HTTP_UNAVAILABLE;

    // Update backoff metrics.
    for (String retryCount : retryCounts) {
      op.updateBackoffMetrics(Integer.parseInt(retryCount), statusCode);
    }

    // For retry count greater than the max configured value, the request should fail.
    Assert.assertEquals("Number of failed requests does not match expected value.",
            "3", String.valueOf(testClient.getAbfsCounters().getAbfsBackoffMetrics().getNumberOfRequestsFailed()));

    // Close the AzureBlobFileSystem.
    fs.close();
  }

}
