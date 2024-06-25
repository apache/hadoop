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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsDriverException;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.KEEP_ALIVE_CACHE_CLOSED;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * This test class tests the exception handling in ABFS thrown by the
 * {@link KeepAliveCache}.
 */
public class ITestApacheClientConnectionPool extends
    AbstractAbfsIntegrationTest {

  public ITestApacheClientConnectionPool() throws Exception {
    super();
  }

  @Test
  public void testKacIsClosed() throws Exception {
    try(AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration())) {
      KeepAliveCache kac = fs.getAbfsStore().getClient().getKeepAliveCache();
      kac.close();
      intercept(AbfsDriverException.class, KEEP_ALIVE_CACHE_CLOSED, () -> {
        fs.create(new Path("/test"));
      });
    }
  }
}
