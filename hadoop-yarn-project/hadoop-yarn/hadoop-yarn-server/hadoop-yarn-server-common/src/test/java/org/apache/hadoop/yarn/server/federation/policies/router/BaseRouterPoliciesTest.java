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

package org.apache.hadoop.yarn.server.federation.policies.router;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.BaseFederationPoliciesTest;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.utils.FederationPoliciesTestUtil;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.when;

/**
 * Base class for router policies tests, tests for null input cases.
 */
public abstract class BaseRouterPoliciesTest
    extends BaseFederationPoliciesTest {

  @Test
  public void testNullQueueRouting() throws YarnException {
    FederationRouterPolicy localPolicy = (FederationRouterPolicy) getPolicy();
    ApplicationSubmissionContext applicationSubmissionContext =
        ApplicationSubmissionContext.newInstance(null, null, null, null, null,
            false, false, 0, Resources.none(), null, false, null, null);
    SubClusterId chosen =
        localPolicy.getHomeSubcluster(applicationSubmissionContext, null);
    Assert.assertNotNull(chosen);
  }

  @Test(expected = FederationPolicyException.class)
  public void testNullAppContext() throws YarnException {
    ((FederationRouterPolicy) getPolicy()).getHomeSubcluster(null, null);
  }

  @Test
  public void testBlacklistSubcluster() throws YarnException {
    FederationRouterPolicy localPolicy = (FederationRouterPolicy) getPolicy();
    ApplicationSubmissionContext applicationSubmissionContext =
        ApplicationSubmissionContext.newInstance(null, null, null, null, null,
            false, false, 0, Resources.none(), null, false, null, null);
    Map<SubClusterId, SubClusterInfo> activeSubClusters =
        getActiveSubclusters();
    if (activeSubClusters != null && activeSubClusters.size() > 1
        && !(localPolicy instanceof RejectRouterPolicy)) {
      // blacklist all the active subcluster but one.
      Random random = new Random();
      List<SubClusterId> blacklistSubclusters =
          new ArrayList<SubClusterId>(activeSubClusters.keySet());
      SubClusterId removed = blacklistSubclusters
          .remove(random.nextInt(blacklistSubclusters.size()));
      // bias LoadBasedRouterPolicy
      getPolicyInfo().getRouterPolicyWeights()
          .put(new SubClusterIdInfo(removed), 1.0f);
      FederationPoliciesTestUtil.initializePolicyContext(getPolicy(),
          getPolicyInfo(), getActiveSubclusters());

      SubClusterId chosen = localPolicy.getHomeSubcluster(
          applicationSubmissionContext, blacklistSubclusters);

      // check that the selected sub-cluster is only one not blacklisted
      Assert.assertNotNull(chosen);
      Assert.assertEquals(removed, chosen);
    }
  }

  /**
   * This test validates the correctness of blacklist logic in case the cluster
   * has no active subclusters.
   */
  @Test
  public void testAllBlacklistSubcluster() throws YarnException {
    FederationRouterPolicy localPolicy = (FederationRouterPolicy) getPolicy();
    ApplicationSubmissionContext applicationSubmissionContext =
        ApplicationSubmissionContext.newInstance(null, null, null, null, null,
            false, false, 0, Resources.none(), null, false, null, null);
    Map<SubClusterId, SubClusterInfo> activeSubClusters =
        getActiveSubclusters();
    if (activeSubClusters != null && activeSubClusters.size() > 1
        && !(localPolicy instanceof RejectRouterPolicy)) {
      List<SubClusterId> blacklistSubclusters =
          new ArrayList<SubClusterId>(activeSubClusters.keySet());
      try {
        localPolicy.getHomeSubcluster(applicationSubmissionContext,
            blacklistSubclusters);
        Assert.fail();
      } catch (YarnException e) {
        Assert.assertTrue(e.getMessage()
            .equals(FederationPolicyUtils.NO_ACTIVE_SUBCLUSTER_AVAILABLE));
      }
    }
  }

  @Test
  public void testNullReservationContext() throws Exception {
    FederationRouterPolicy policy = ((FederationRouterPolicy) getPolicy());

    LambdaTestUtils.intercept(FederationPolicyException.class,
        "The ReservationSubmissionRequest cannot be null.",
        () -> policy.getReservationHomeSubcluster(null));
  }

  @Test
  public void testUnknownReservation() throws Exception {

    long now = Time.now();
    ReservationSubmissionRequest resReq = getReservationSubmissionRequest();
    ReservationId reservationId = ReservationId.newInstance(now, 1);
    when(resReq.getQueue()).thenReturn("queue1");
    when(resReq.getReservationId()).thenReturn(reservationId);

    // route an application that uses this app
    ApplicationSubmissionContext applicationSubmissionContext =
        ApplicationSubmissionContext.newInstance(
            ApplicationId.newInstance(now, 1), "app1", "queue1", Priority.newInstance(1),
                null, false, false, 1, null, null, false);

    applicationSubmissionContext.setReservationID(resReq.getReservationId());
    FederationRouterPolicy policy = (FederationRouterPolicy) getPolicy();

    LambdaTestUtils.intercept(YarnException.class,
        "Reservation " + reservationId + " does not exist",
        () -> policy.getHomeSubcluster(applicationSubmissionContext, new ArrayList<>()));
  }

  @Test
  public void testFollowReservation() throws YarnException {

    long now = Time.now();
    ReservationSubmissionRequest resReq = getReservationSubmissionRequest();
    when(resReq.getQueue()).thenReturn("queue1");
    when(resReq.getReservationId()).thenReturn(ReservationId.newInstance(now, 1));

    FederationRouterPolicy routerPolicy = (FederationRouterPolicy) getPolicy();
    FederationPolicyInitializationContext fdContext = getFederationPolicyContext();
    FederationStateStoreFacade storeFacade = fdContext.getFederationStateStoreFacade();

    // first we invoke a reservation placement
    SubClusterId chosen = routerPolicy.getReservationHomeSubcluster(resReq);

    // add this to the store
    ReservationHomeSubCluster homeSubCluster =
        ReservationHomeSubCluster.newInstance(resReq.getReservationId(), chosen);
    storeFacade.addReservationHomeSubCluster(homeSubCluster);

    // route an application that uses this app
    ApplicationSubmissionContext applicationSubmissionContext =
        ApplicationSubmissionContext.newInstance(
            ApplicationId.newInstance(now, 1), "app1", "queue1", Priority.newInstance(1),
                null, false, false, 1, null, null, false);

    applicationSubmissionContext.setReservationID(resReq.getReservationId());
    SubClusterId chosen2 = routerPolicy.getHomeSubcluster(
        applicationSubmissionContext, Collections.emptyList());

    // application follows reservation
    Assert.assertEquals(chosen, chosen2);
  }

  @Test
  public void testUpdateReservation() throws YarnException {
    long now = Time.now();
    ReservationSubmissionRequest resReq = getReservationSubmissionRequest();
    when(resReq.getQueue()).thenReturn("queue1");
    when(resReq.getReservationId()).thenReturn(ReservationId.newInstance(now, 1));

    // first we invoke a reservation placement
    FederationRouterPolicy routerPolicy = (FederationRouterPolicy) getPolicy();
    SubClusterId chosen = routerPolicy.getReservationHomeSubcluster(resReq);

    // add this to the store
    FederationStateStoreFacade facade =
        getFederationPolicyContext().getFederationStateStoreFacade();
    ReservationHomeSubCluster subCluster =
        ReservationHomeSubCluster.newInstance(resReq.getReservationId(), chosen);
    facade.addReservationHomeSubCluster(subCluster);

    // get all activeSubClusters
    Map<SubClusterId, SubClusterInfo> activeSubClusters = getActiveSubclusters();

    // Update ReservationHomeSubCluster
    List<SubClusterId> subClusterIds = new ArrayList<>(activeSubClusters.keySet());
    SubClusterId chosen2 = subClusterIds.get(this.getRand().nextInt(subClusterIds.size()));
    ReservationHomeSubCluster subCluster2 =
        ReservationHomeSubCluster.newInstance(resReq.getReservationId(), chosen2);
    facade.updateReservationHomeSubCluster(subCluster2);

    // route an application that uses this app
    ApplicationSubmissionContext applicationSubmissionContext =
        ApplicationSubmissionContext.newInstance(
            ApplicationId.newInstance(now, 1), "app1", "queue1", Priority.newInstance(1),
                 null, false, false, 1, null, null, false);

    applicationSubmissionContext.setReservationID(resReq.getReservationId());
    SubClusterId chosen3 = routerPolicy.getHomeSubcluster(
        applicationSubmissionContext, new ArrayList<>());

    Assert.assertEquals(chosen2, chosen3);
  }

  @Test
  public void testDeleteReservation() throws Exception {
    long now = Time.now();
    ReservationSubmissionRequest resReq = getReservationSubmissionRequest();
    when(resReq.getQueue()).thenReturn("queue1");
    when(resReq.getReservationId()).thenReturn(ReservationId.newInstance(now, 1));

    // first we invoke a reservation placement
    FederationRouterPolicy routerPolicy = (FederationRouterPolicy) getPolicy();
    SubClusterId chosen = routerPolicy.getReservationHomeSubcluster(resReq);

    // add this to the store
    FederationStateStoreFacade facade =
        getFederationPolicyContext().getFederationStateStoreFacade();
    ReservationHomeSubCluster subCluster =
        ReservationHomeSubCluster.newInstance(resReq.getReservationId(), chosen);
    facade.addReservationHomeSubCluster(subCluster);

    // delete this to the store
    facade.deleteReservationHomeSubCluster(resReq.getReservationId());

    ApplicationSubmissionContext applicationSubmissionContext =
        ApplicationSubmissionContext.newInstance(
            ApplicationId.newInstance(now, 1), "app1", "queue1", Priority.newInstance(1),
                null, false, false, 1, null, null, false);
    applicationSubmissionContext.setReservationID(resReq.getReservationId());

    LambdaTestUtils.intercept(YarnException.class,
        "Reservation " + resReq.getReservationId() + " does not exist",
        () -> routerPolicy.getHomeSubcluster(applicationSubmissionContext, new ArrayList<>()));
  }
}
