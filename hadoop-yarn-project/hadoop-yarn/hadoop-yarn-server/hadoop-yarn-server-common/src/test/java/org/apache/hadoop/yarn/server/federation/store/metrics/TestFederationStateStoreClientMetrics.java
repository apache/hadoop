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

package org.apache.hadoop.yarn.server.federation.store.metrics;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unittests for {@link FederationStateStoreClientMetrics}.
 *
 */
public class TestFederationStateStoreClientMetrics {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestFederationStateStoreClientMetrics.class);

  private MockBadFederationStateStore badStateStore =
      new MockBadFederationStateStore();
  private MockGoodFederationStateStore goodStateStore =
      new MockGoodFederationStateStore();

  @Test
  public void testAggregateMetricInit() {
    LOG.info("Test: aggregate metrics are initialized correctly");

    Assert.assertEquals(0,
        FederationStateStoreClientMetrics.getNumSucceededCalls());
    Assert.assertEquals(0,
        FederationStateStoreClientMetrics.getNumFailedCalls());

    LOG.info("Test: aggregate metrics are updated correctly");
  }

  @Test
  public void testSuccessfulCalls() {
    LOG.info("Test: Aggregate and method successful calls updated correctly");

    long totalGoodBefore =
        FederationStateStoreClientMetrics.getNumSucceededCalls();
    long apiGoodBefore = FederationStateStoreClientMetrics
        .getNumSucceessfulCallsForMethod("registerSubCluster");

    goodStateStore.registerSubCluster(100);

    Assert.assertEquals(totalGoodBefore + 1,
        FederationStateStoreClientMetrics.getNumSucceededCalls());
    Assert.assertEquals(100,
        FederationStateStoreClientMetrics.getLatencySucceededCalls(), 0);
    Assert.assertEquals(apiGoodBefore + 1,
        FederationStateStoreClientMetrics.getNumSucceededCalls());
    Assert.assertEquals(100, FederationStateStoreClientMetrics
        .getLatencySucceessfulCallsForMethod("registerSubCluster"), 0);

    LOG.info("Test: Running stats correctly calculated for 2 metrics");

    goodStateStore.registerSubCluster(200);

    Assert.assertEquals(totalGoodBefore + 2,
        FederationStateStoreClientMetrics.getNumSucceededCalls());
    Assert.assertEquals(150,
        FederationStateStoreClientMetrics.getLatencySucceededCalls(), 0);
    Assert.assertEquals(apiGoodBefore + 2,
        FederationStateStoreClientMetrics.getNumSucceededCalls());
    Assert.assertEquals(150, FederationStateStoreClientMetrics
        .getLatencySucceessfulCallsForMethod("registerSubCluster"), 0);

  }

  @Test
  public void testFailedCalls() {

    long totalBadbefore = FederationStateStoreClientMetrics.getNumFailedCalls();
    long apiBadBefore = FederationStateStoreClientMetrics
        .getNumFailedCallsForMethod("registerSubCluster");

    badStateStore.registerSubCluster();

    LOG.info("Test: Aggregate and method failed calls updated correctly");
    Assert.assertEquals(totalBadbefore + 1,
        FederationStateStoreClientMetrics.getNumFailedCalls());
    Assert.assertEquals(apiBadBefore + 1, FederationStateStoreClientMetrics
        .getNumFailedCallsForMethod("registerSubCluster"));

  }

  @Test
  public void testCallsUnknownMethod() {

    long totalBadbefore = FederationStateStoreClientMetrics.getNumFailedCalls();
    long apiBadBefore = FederationStateStoreClientMetrics
        .getNumFailedCallsForMethod("registerSubCluster");
    long totalGoodBefore =
        FederationStateStoreClientMetrics.getNumSucceededCalls();
    long apiGoodBefore = FederationStateStoreClientMetrics
        .getNumSucceessfulCallsForMethod("registerSubCluster");

    LOG.info("Calling Metrics class directly");
    FederationStateStoreClientMetrics.failedStateStoreCall();
    FederationStateStoreClientMetrics.succeededStateStoreCall(100);

    LOG.info("Test: Aggregate and method calls did not update");
    Assert.assertEquals(totalBadbefore,
        FederationStateStoreClientMetrics.getNumFailedCalls());
    Assert.assertEquals(apiBadBefore, FederationStateStoreClientMetrics
        .getNumFailedCallsForMethod("registerSubCluster"));

    Assert.assertEquals(totalGoodBefore,
        FederationStateStoreClientMetrics.getNumSucceededCalls());
    Assert.assertEquals(apiGoodBefore, FederationStateStoreClientMetrics
        .getNumSucceessfulCallsForMethod("registerSubCluster"));

  }

  // Records failures for all calls
  private class MockBadFederationStateStore {
    public void registerSubCluster() {
      LOG.info("Mocked: failed registerSubCluster call");
      FederationStateStoreClientMetrics.failedStateStoreCall();
    }
  }

  // Records successes for all calls
  private class MockGoodFederationStateStore {
    public void registerSubCluster(long duration) {
      LOG.info("Mocked: successful registerSubCluster call with duration {}",
          duration);
      FederationStateStoreClientMetrics.succeededStateStoreCall(duration);
    }
  }
}
