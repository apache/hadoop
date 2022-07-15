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
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_DELETE;
import static org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType.DeletePath;
import java.lang.reflect.Method;
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

  @Test
  public void testDriverRetryMetrics() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    AbfsClient testClient = super.getAbfsClient(super.getAbfsStore(getFileSystem()));

    // Mock instance of AbfsRestOperation
    AbfsRestOperation op = TestAbfsClient.getRestOp(
        DeletePath, testClient, HTTP_METHOD_DELETE,
       TestAbfsClient.getTestUrl(testClient, "/NonExistingPath"), TestAbfsClient.getTestRequestHeaders(testClient));

    ArrayList<Integer> retryCounts = new ArrayList<>(Arrays.asList(35, 28, 31, 45, 10, 2, 9));
    int statusCode = HttpURLConnection.HTTP_UNAVAILABLE;
    Method getMetrics = AbfsRestOperation.class.getDeclaredMethod("updateDriverMetrics", int.class, int.class);
    getMetrics.setAccessible(true);
    for (int retryCount : retryCounts) {
      getMetrics.invoke(op, retryCount, statusCode);
    }
    //For retry count greater than max configured value, the request should fail
    Assert.assertEquals(testClient.getAbfsCounters().getAbfsDriverMetrics().getNumberOfRequestsFailed().toString(), "3");
    fs.close();
  }
}
