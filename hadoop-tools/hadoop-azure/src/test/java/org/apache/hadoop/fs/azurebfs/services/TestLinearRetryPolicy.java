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

import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_RESET_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_TIMEOUT_ABBREVIATION;

public class TestLinearRetryPolicy extends AbstractAbfsIntegrationTest {

  public TestLinearRetryPolicy() throws Exception {
    super();
  }

  @Test
  public void testLinearRetryPolicyInitialization() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    AbfsClient client = fs.getAbfsStore().getClient();

    RetryPolicy retryPolicy = client.getRetryPolicy(CONNECTION_TIMEOUT_ABBREVIATION);
    assertTrue(retryPolicy instanceof LinearRetryPolicy);

    retryPolicy = client.getRetryPolicy(CONNECTION_RESET_ABBREVIATION);
    assertTrue(retryPolicy instanceof ExponentialRetryPolicy);
  }

  @Test
  public void testLinearRetryIntervalWithDoubleStepUpEnabled() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    AbfsClient client = fs.getAbfsStore().getClient();

    RetryPolicy retryPolicy = client.getRetryPolicy(CONNECTION_TIMEOUT_ABBREVIATION);
    assertTrue(retryPolicy instanceof LinearRetryPolicy);
    assertTrue(((LinearRetryPolicy) retryPolicy).getDoubleStepUpEnabled());

    assertEquals(500, retryPolicy.getRetryInterval(0));
    assertEquals(1000, retryPolicy.getRetryInterval(1));
    assertEquals(2000, retryPolicy.getRetryInterval(2));
    assertEquals(4000, retryPolicy.getRetryInterval(3));
  }

  @Test
  public void testLinearRetryIntervalWithDoubleStepUpDisabled() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    AbfsClient client = fs.getAbfsStore().getClient();
    client.getAbfsConfiguration().setLinearRetryDoubleStepUpEnabled(false);

    RetryPolicy retryPolicy = new LinearRetryPolicy(client.getAbfsConfiguration());
    assertTrue(retryPolicy instanceof LinearRetryPolicy);
    assertFalse(((LinearRetryPolicy) retryPolicy).getDoubleStepUpEnabled());

    assertEquals(500, retryPolicy.getRetryInterval(0));
    assertEquals(1500, retryPolicy.getRetryInterval(1));
    assertEquals(2500, retryPolicy.getRetryInterval(2));
    assertEquals(3500, retryPolicy.getRetryInterval(3));
  }
}
