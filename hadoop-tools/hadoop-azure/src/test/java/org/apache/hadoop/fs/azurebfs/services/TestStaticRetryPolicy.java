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

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_LINEAR_RETRY_DOUBLE_STEP_UP_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_LINEAR_RETRY_FOR_CONNECTION_TIMEOUT_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_STATIC_RETRY_FOR_CONNECTION_TIMEOUT_ENABLED;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_RESET_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_TIMEOUT_ABBREVIATION;

/**
 * Class to test the behavior of Linear Retry policy as well the inheritance
 * between {@link RetryPolicy}, {@link LinearRetryPolicy}, {@link ExponentialRetryPolicy}
 */
public class TestStaticRetryPolicy extends AbstractAbfsIntegrationTest {

  private final int STATIC_RETRY_INTERVAL = 2000;

  public TestStaticRetryPolicy() throws Exception {
    super();
  }

  @Test
  public void testStaticRetryPolicyInitialization() throws Exception {
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set(AZURE_STATIC_RETRY_FOR_CONNECTION_TIMEOUT_ENABLED, "true");
    final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem
        .newInstance(getFileSystem().getUri(), config);
    AbfsClient client = fs.getAbfsStore().getClient();

    // Assert that static retry policy will be used only for CT Failures
    RetryPolicy retryPolicy = client.getRetryPolicy(CONNECTION_TIMEOUT_ABBREVIATION);
    Assertions.assertThat(retryPolicy).isInstanceOf(StaticRetryPolicy.class);

    // For all other possible values of failureReason, Exponential retry is used
    retryPolicy = client.getRetryPolicy("");
    Assertions.assertThat(retryPolicy).isInstanceOf(ExponentialRetryPolicy.class);
    retryPolicy = client.getRetryPolicy(null);
    Assertions.assertThat(retryPolicy).isInstanceOf(ExponentialRetryPolicy.class);
    retryPolicy = client.getRetryPolicy(CONNECTION_RESET_ABBREVIATION);
    Assertions.assertThat(retryPolicy).isInstanceOf(ExponentialRetryPolicy.class);
  }

  @Test
  public void testStaticRetryInterval() throws Exception {
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set(AZURE_STATIC_RETRY_FOR_CONNECTION_TIMEOUT_ENABLED, "true");
    final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem
        .newInstance(getFileSystem().getUri(), config);
    AbfsClient client = fs.getAbfsStore().getClient();

    RetryPolicy retryPolicy = client.getRetryPolicy(CONNECTION_TIMEOUT_ABBREVIATION);
    Assertions.assertThat(retryPolicy).isInstanceOf(StaticRetryPolicy.class);

    Assertions.assertThat(retryPolicy.getRetryInterval(0))
        .isEqualTo(STATIC_RETRY_INTERVAL);
    Assertions.assertThat(retryPolicy.getRetryInterval(1))
        .isEqualTo(STATIC_RETRY_INTERVAL);
    Assertions.assertThat(retryPolicy.getRetryInterval(2))
        .isEqualTo(STATIC_RETRY_INTERVAL);
    Assertions.assertThat(retryPolicy.getRetryInterval(3))
        .isEqualTo(STATIC_RETRY_INTERVAL);
  }

  @Test
  public void testLinearRetryConfigurations() throws Exception {
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set(AZURE_LINEAR_RETRY_FOR_CONNECTION_TIMEOUT_ENABLED, "false");
    config.set(AZURE_STATIC_RETRY_FOR_CONNECTION_TIMEOUT_ENABLED, "false");
    final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem
        .newInstance(getFileSystem().getUri(), config);
    AbfsClient client = fs.getAbfsStore().getClient();

    // Assert that linear retry policy is disabled even for CT
    RetryPolicy retryPolicy = client.getRetryPolicy(CONNECTION_TIMEOUT_ABBREVIATION);
    Assertions.assertThat(retryPolicy).isInstanceOf(ExponentialRetryPolicy.class);
    retryPolicy = client.getRetryPolicy("");
    Assertions.assertThat(retryPolicy).isInstanceOf(ExponentialRetryPolicy.class);
    retryPolicy = client.getRetryPolicy(null);
    Assertions.assertThat(retryPolicy).isInstanceOf(ExponentialRetryPolicy.class);
    retryPolicy = client.getRetryPolicy(CONNECTION_RESET_ABBREVIATION);
    Assertions.assertThat(retryPolicy).isInstanceOf(ExponentialRetryPolicy.class);
  }
}
