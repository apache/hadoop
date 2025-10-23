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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestAMRMProxyMetrics extends BaseAMRMProxyTest {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestAMRMProxyMetrics.class);
  private static AMRMProxyMetrics metrics;

  @BeforeAll
  public static void init() {
    metrics = AMRMProxyMetrics.getMetrics();
    LOG.info("Test: aggregate metrics are initialized correctly");

    assertEquals(0, metrics.getFailedAppStartRequests());
    assertEquals(0, metrics.getFailedRegisterAMRequests());
    assertEquals(0, metrics.getFailedFinishAMRequests());
    assertEquals(0, metrics.getFailedAllocateRequests());
    assertEquals(0, metrics.getFailedAppRecoveryCount());
    assertEquals(0, metrics.getFailedAppStopRequests());
    assertEquals(0, metrics.getFailedUpdateAMRMTokenRequests());
    assertEquals(0, metrics.getAllocateCount());
    assertEquals(0, metrics.getRequestCount());

    assertEquals(0, metrics.getNumSucceededAppStartRequests());
    assertEquals(0, metrics.getNumSucceededRegisterAMRequests());
    assertEquals(0, metrics.getNumSucceededFinishAMRequests());
    assertEquals(0, metrics.getNumSucceededAllocateRequests());
    assertEquals(0, metrics.getNumSucceededRecoverRequests());
    assertEquals(0, metrics.getNumSucceededAppStopRequests());
    assertEquals(0, metrics.getNumSucceededUpdateAMRMTokenRequests());

    LOG.info("Test: aggregate metrics are updated correctly");
  }

  @Test
  public void testAllocateRequestWithNullValues() throws Exception {
    long failedAppStartRequests = metrics.getFailedAppStartRequests();
    long failedRegisterAMRequests = metrics.getFailedRegisterAMRequests();
    long failedFinishAMRequests = metrics.getFailedFinishAMRequests();
    long failedAllocateRequests = metrics.getFailedAllocateRequests();
    long failedAppRecoveryRequests = metrics.getFailedAppRecoveryCount();
    long failedAppStopRequests = metrics.getFailedAppStopRequests();
    long failedUpdateAMRMTokenRequests = metrics.getFailedUpdateAMRMTokenRequests();

    long succeededAppStartRequests = metrics.getNumSucceededAppStartRequests();
    long succeededRegisterAMRequests = metrics.getNumSucceededRegisterAMRequests();
    long succeededFinishAMRequests = metrics.getNumSucceededFinishAMRequests();
    long succeededAllocateRequests = metrics.getNumSucceededAllocateRequests();

    int testAppId = 1;
    RegisterApplicationMasterResponse registerResponse = registerApplicationMaster(testAppId);
    assertNotNull(registerResponse);
    assertEquals(Integer.toString(testAppId), registerResponse.getQueue());

    AllocateResponse allocateResponse = allocate(testAppId);
    assertNotNull(allocateResponse);

    FinishApplicationMasterResponse finishResponse =
        finishApplicationMaster(testAppId, FinalApplicationStatus.SUCCEEDED);

    assertNotNull(finishResponse);
    assertTrue(finishResponse.getIsUnregistered());

    assertEquals(failedAppStartRequests, metrics.getFailedAppStartRequests());
    assertEquals(failedRegisterAMRequests, metrics.getFailedRegisterAMRequests());
    assertEquals(failedFinishAMRequests, metrics.getFailedFinishAMRequests());
    assertEquals(failedAllocateRequests, metrics.getFailedAllocateRequests());
    assertEquals(failedAppRecoveryRequests, metrics.getFailedAppRecoveryCount());
    assertEquals(failedAppStopRequests, metrics.getFailedAppStopRequests());
    assertEquals(failedUpdateAMRMTokenRequests, metrics.getFailedUpdateAMRMTokenRequests());

    assertEquals(succeededAppStartRequests,
        metrics.getNumSucceededAppStartRequests());
    assertEquals(1 + succeededRegisterAMRequests,
        metrics.getNumSucceededRegisterAMRequests());
    assertEquals(1 + succeededFinishAMRequests,
        metrics.getNumSucceededFinishAMRequests());
    assertEquals(1 + succeededAllocateRequests,
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
    assertNotNull(registerResponse);
    assertEquals(Integer.toString(testAppId), registerResponse.getQueue());

    FinishApplicationMasterResponse finishResponse =
        finishApplicationMaster(testAppId, FinalApplicationStatus.FAILED);

    assertNotNull(finishResponse);

    try {
      // Try to finish an application master that is already finished.
      finishApplicationMaster(testAppId, FinalApplicationStatus.SUCCEEDED);
      fail("The request to finish application master should have failed");
    } catch (Throwable ex) {
      // This is expected. So nothing required here.
      LOG.info("Finish registration failed as expected because it was not "
          + "registered");
    }

    assertEquals(failedAppStartRequests,
        metrics.getFailedAppStartRequests());
    assertEquals(failedRegisterAMRequests,
        metrics.getFailedRegisterAMRequests());
    assertEquals(1 + failedFinishAMRequests,
        metrics.getFailedFinishAMRequests());
    assertEquals(failedAllocateRequests,
        metrics.getFailedAllocateRequests());

    assertEquals(succeededAppStartRequests,
        metrics.getNumSucceededAppStartRequests());
    assertEquals(1 + succeededRegisterAMRequests,
        metrics.getNumSucceededRegisterAMRequests());
    assertEquals(1 + succeededFinishAMRequests,
        metrics.getNumSucceededFinishAMRequests());
    assertEquals(succeededAllocateRequests,
        metrics.getNumSucceededAllocateRequests());
  }
}
