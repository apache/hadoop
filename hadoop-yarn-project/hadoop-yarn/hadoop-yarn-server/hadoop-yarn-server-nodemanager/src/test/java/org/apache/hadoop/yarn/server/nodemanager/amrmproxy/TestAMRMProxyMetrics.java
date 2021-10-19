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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAMRMProxyMetrics extends BaseAMRMProxyTest {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestAMRMProxyMetrics.class);
  private static AMRMProxyMetrics metrics;

  @BeforeClass
  public static void init() {
    metrics = AMRMProxyMetrics.getMetrics();
    LOG.info("Test: aggregate metrics are initialized correctly");

    Assert.assertEquals(0, metrics.getFailedAppStartRequests());
    Assert.assertEquals(0, metrics.getFailedRegisterAMRequests());
    Assert.assertEquals(0, metrics.getFailedFinishAMRequests());
    Assert.assertEquals(0, metrics.getFailedAllocateRequests());

    Assert.assertEquals(0, metrics.getNumSucceededAppStartRequests());
    Assert.assertEquals(0, metrics.getNumSucceededRegisterAMRequests());
    Assert.assertEquals(0, metrics.getNumSucceededFinishAMRequests());
    Assert.assertEquals(0, metrics.getNumSucceededAllocateRequests());

    LOG.info("Test: aggregate metrics are updated correctly");
  }

  @Test
  public void testAllocateRequestWithNullValues() throws Exception {
    long failedAppStartRequests = metrics.getFailedAppStartRequests();
    long failedRegisterAMRequests = metrics.getFailedRegisterAMRequests();
    long failedFinishAMRequests = metrics.getFailedFinishAMRequests();
    long failedAllocateRequests = metrics.getFailedAllocateRequests();

    long succeededAppStartRequests = metrics.getNumSucceededAppStartRequests();
    long succeededRegisterAMRequests =
        metrics.getNumSucceededRegisterAMRequests();
    long succeededFinishAMRequests = metrics.getNumSucceededFinishAMRequests();
    long succeededAllocateRequests = metrics.getNumSucceededAllocateRequests();

    int testAppId = 1;
    RegisterApplicationMasterResponse registerResponse =
        registerApplicationMaster(testAppId);
    Assert.assertNotNull(registerResponse);
    Assert
        .assertEquals(Integer.toString(testAppId), registerResponse.getQueue());

    AllocateResponse allocateResponse = allocate(testAppId);
    Assert.assertNotNull(allocateResponse);

    FinishApplicationMasterResponse finshResponse =
        finishApplicationMaster(testAppId, FinalApplicationStatus.SUCCEEDED);

    Assert.assertNotNull(finshResponse);
    Assert.assertEquals(true, finshResponse.getIsUnregistered());

    Assert.assertEquals(failedAppStartRequests,
        metrics.getFailedAppStartRequests());
    Assert.assertEquals(failedRegisterAMRequests,
        metrics.getFailedRegisterAMRequests());
    Assert.assertEquals(failedFinishAMRequests,
        metrics.getFailedFinishAMRequests());
    Assert.assertEquals(failedAllocateRequests,
        metrics.getFailedAllocateRequests());

    Assert.assertEquals(succeededAppStartRequests,
        metrics.getNumSucceededAppStartRequests());
    Assert.assertEquals(1 + succeededRegisterAMRequests,
        metrics.getNumSucceededRegisterAMRequests());
    Assert.assertEquals(1 + succeededFinishAMRequests,
        metrics.getNumSucceededFinishAMRequests());
    Assert.assertEquals(1 + succeededAllocateRequests,
        metrics.getNumSucceededAllocateRequests());
  }

  @Test
  public void testFinishOneApplicationMasterWithFailure() throws Exception {
    long failedAppStartRequests = metrics.getFailedAppStartRequests();
    long failedRegisterAMRequests = metrics.getFailedRegisterAMRequests();
    long failedFinishAMRequests = metrics.getFailedFinishAMRequests();
    long failedAllocateRequests = metrics.getFailedAllocateRequests();

    long succeededAppStartRequests = metrics.getNumSucceededAppStartRequests();
    long succeededRegisterAMRequests =
        metrics.getNumSucceededRegisterAMRequests();
    long succeededFinishAMRequests = metrics.getNumSucceededFinishAMRequests();
    long succeededAllocateRequests = metrics.getNumSucceededAllocateRequests();

    int testAppId = 1;
    RegisterApplicationMasterResponse registerResponse =
        registerApplicationMaster(testAppId);
    Assert.assertNotNull(registerResponse);
    Assert
        .assertEquals(Integer.toString(testAppId), registerResponse.getQueue());

    FinishApplicationMasterResponse finshResponse =
        finishApplicationMaster(testAppId, FinalApplicationStatus.FAILED);

    Assert.assertNotNull(finshResponse);

    try {
      // Try to finish an application master that is already finished.
      finishApplicationMaster(testAppId, FinalApplicationStatus.SUCCEEDED);
      Assert
          .fail("The request to finish application master should have failed");
    } catch (Throwable ex) {
      // This is expected. So nothing required here.
      LOG.info("Finish registration failed as expected because it was not "
          + "registered");
    }

    Assert.assertEquals(failedAppStartRequests,
        metrics.getFailedAppStartRequests());
    Assert.assertEquals(failedRegisterAMRequests,
        metrics.getFailedRegisterAMRequests());
    Assert.assertEquals(1 + failedFinishAMRequests,
        metrics.getFailedFinishAMRequests());
    Assert.assertEquals(failedAllocateRequests,
        metrics.getFailedAllocateRequests());

    Assert.assertEquals(succeededAppStartRequests,
        metrics.getNumSucceededAppStartRequests());
    Assert.assertEquals(1 + succeededRegisterAMRequests,
        metrics.getNumSucceededRegisterAMRequests());
    Assert.assertEquals(1 + succeededFinishAMRequests,
        metrics.getNumSucceededFinishAMRequests());
    Assert.assertEquals(succeededAllocateRequests,
        metrics.getNumSucceededAllocateRequests());
  }
}
