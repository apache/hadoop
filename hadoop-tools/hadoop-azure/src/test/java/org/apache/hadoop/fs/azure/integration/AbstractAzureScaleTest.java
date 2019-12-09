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

package org.apache.hadoop.fs.azure.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azure.AbstractWasbTestBase;
import org.apache.hadoop.fs.azure.AzureBlobStorageTestAccount;

import static org.apache.hadoop.fs.azure.integration.AzureTestUtils.*;

/**
 * Scale tests are only executed if the scale profile
 * is set; the setup method will check this and skip
 * tests if not.
 *
 */
public abstract class AbstractAzureScaleTest
    extends AbstractWasbTestBase implements Sizes {

  protected static final Logger LOG =
      LoggerFactory.getLogger(AbstractAzureScaleTest.class);

  @Override
  protected int getTestTimeoutMillis() {
    return AzureTestConstants.SCALE_TEST_TIMEOUT_MILLIS;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    LOG.debug("Scale test operation count = {}", getOperationCount());
    assumeScaleTestsEnabled(getConfiguration());
  }

  /**
   * Create the test account.
   * @return a test account
   * @throws Exception on any failure to create the account.
   */
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create(createConfiguration());
  }

  protected long getOperationCount() {
    return getConfiguration().getLong(KEY_OPERATION_COUNT,
        DEFAULT_OPERATION_COUNT);
  }
}
