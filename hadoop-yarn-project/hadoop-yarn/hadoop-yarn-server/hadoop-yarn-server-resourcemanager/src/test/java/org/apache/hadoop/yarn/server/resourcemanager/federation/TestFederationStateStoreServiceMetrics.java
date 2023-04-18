/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.federation;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for TestFederationStateStoreServiceMetrics.
 */
public class TestFederationStateStoreServiceMetrics {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestFederationStateStoreServiceMetrics.class);

  private static FederationStateStoreServiceMetrics metrics =
      FederationStateStoreServiceMetrics.getMetrics();

  private MockBadFederationStateStoreService badStateStore =
      new MockBadFederationStateStoreService();
  private MockGoodFederationStateStoreService goodStateStore =
      new MockGoodFederationStateStoreService();

  // Records failures for all calls
  private class MockBadFederationStateStoreService {
    public void registerSubCluster() {
      LOG.info("Mocked: failed registerSubCluster call");
      FederationStateStoreServiceMetrics.failedStateStoreServiceCall();
    }
  }

  // Records successes for all calls
  private class MockGoodFederationStateStoreService {
    public void registerSubCluster(long duration) {
      LOG.info("Mocked: successful registerSubCluster call with duration {}", duration);
      FederationStateStoreServiceMetrics.succeededStateStoreServiceCall(duration);
    }
  }

  @Test
  public void testFederationStateStoreServiceMetricInit() {
    LOG.info("Test: aggregate metrics are initialized correctly");
    assertEquals(0, FederationStateStoreServiceMetrics.getNumSucceededCalls());
    assertEquals(0, FederationStateStoreServiceMetrics.getNumFailedCalls());
    LOG.info("Test: aggregate metrics are updated correctly");
  }

  @Test
  public void testRegisterSubClusterSuccessfulCalls() {
    LOG.info("Test: Aggregate and method successful calls updated correctly.");

    long totalGoodBefore = FederationStateStoreServiceMetrics.getNumSucceededCalls();
    long apiGoodBefore = FederationStateStoreServiceMetrics.
        getNumSucceessfulCallsForMethod("registerSubCluster");

    // Call the registerSubCluster method
    goodStateStore.registerSubCluster(100);

    assertEquals(totalGoodBefore + 1,
        FederationStateStoreServiceMetrics.getNumSucceededCalls());
    assertEquals(100, FederationStateStoreServiceMetrics.getLatencySucceededCalls(), 0);
    assertEquals(apiGoodBefore + 1,
        FederationStateStoreServiceMetrics.getNumSucceededCalls());
    double latencySucceessfulCalls =
        FederationStateStoreServiceMetrics.getLatencySucceessfulCallsForMethod(
        "registerSubCluster");
    assertEquals(100, latencySucceessfulCalls, 0);

    LOG.info("Test: Running stats correctly calculated for 2 metrics");

    // Call the registerSubCluster method
    goodStateStore.registerSubCluster(200);

    assertEquals(totalGoodBefore + 2,
        FederationStateStoreServiceMetrics.getNumSucceededCalls());
    assertEquals(150, FederationStateStoreServiceMetrics.getLatencySucceededCalls(), 0);
    assertEquals(apiGoodBefore + 2,
        FederationStateStoreServiceMetrics.getNumSucceededCalls());
    double latencySucceessfulCalls2 =
        FederationStateStoreServiceMetrics.getLatencySucceessfulCallsForMethod(
        "registerSubCluster");
    assertEquals(150, latencySucceessfulCalls2, 0);
  }
}
