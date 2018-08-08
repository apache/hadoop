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

package org.apache.hadoop.fs.azurebfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azure.integration.AzureTestConstants;

import static org.apache.hadoop.fs.azure.integration.AzureTestUtils.assumeScaleTestsEnabled;

/**
 * Integration tests at bigger scale; configurable as to
 * size, off by default.
 */
public class AbstractAbfsScaleTest extends AbstractAbfsIntegrationTest  {

  protected static final Logger LOG =
      LoggerFactory.getLogger(AbstractAbfsScaleTest.class);

  @Override
  protected int getTestTimeoutMillis() {
    return AzureTestConstants.SCALE_TEST_TIMEOUT_MILLIS;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    LOG.debug("Scale test operation count = {}", getOperationCount());
    assumeScaleTestsEnabled(getConfiguration());
  }

  protected long getOperationCount() {
    return getConfiguration().getLong(AzureTestConstants.KEY_OPERATION_COUNT,
        AzureTestConstants.DEFAULT_OPERATION_COUNT);
  }
}
