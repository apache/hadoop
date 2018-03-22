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

package org.apache.hadoop.yarn.server.federation.policies.amrmproxy;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.BaseFederationPoliciesTest;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.resolver.DefaultSubClusterResolverImpl;
import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.utils.FederationPoliciesTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple test class for the {@link LocalityMulticastAMRMProxyPolicy}.
 */
public class TestLocalityMulticastAMRMProxyPolicy
    extends BaseFederationPoliciesTest {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestLocalityMulticastAMRMProxyPolicy.class);

  @Before
  public void setUp() throws Exception {
    setPolicy(new LocalityMulticastAMRMProxyPolicy());
    setPolicyInfo(new WeightedPolicyInfo());
    Map<SubClusterIdInfo, Float> routerWeights = new HashMap<>();
    Map<SubClusterIdInfo, Float> amrmWeights = new HashMap<>();

    // simulate 20 subclusters with a 5% chance of being inactive
    for (int i = 0; i < 6; i++) {
      SubClusterIdInfo sc = new SubClusterIdInfo("subcluster" + i);
      // sub-cluster 3 is not active
      if (i != 3) {
        SubClusterInfo sci = mock(SubClusterInfo.class);
        when(sci.getState()).thenReturn(SubClusterState.SC_RUNNING);
        when(sci.getSubClusterId()).thenReturn(sc.toId());
        getActiveSubclusters().put(sc.toId(), sci);
      }

      float weight = 1 / 10f;
      routerWeights.put(sc, weight);
      amrmWeights.put(sc, weight);
      // sub-cluster 4 is "disabled" in the weights
      if (i == 4) {
        routerWeights.put(sc, 0f);
        amrmWeights.put(sc, 0f);
      }
    }

    getPolicyInfo().setRouterPolicyWeights(routerWeights);
    getPolicyInfo().setAMRMPolicyWeights(amrmWeights);
    getPolicyInfo().setHeadroomAlpha(0.5f);
    setHomeSubCluster(SubClusterId.newInstance("homesubcluster"));

  }

  @Test
  public void testReinitilialize() throws YarnException {
    initializePolicy();
  }

  private void initializePolicy() throws YarnException {
    setFederationPolicyContext(new FederationPolicyInitializationContext());
    SubClusterResolver resolver = FederationPoliciesTestUtil.initResolver();
    getFederationPolicyContext().setFederationSubclusterResolver(resolver);
    ByteBuffer buf = getPolicyInfo().toByteBuffer();
    getFederationPolicyContext().setSubClusterPolicyConfiguration(
        SubClusterPolicyConfiguration.newInstance("queue1",
            getPolicy().getClass().getCanonicalName(), buf));
    getFederationPolicyContext().setHomeSubcluster(getHomeSubCluster());
    FederationPoliciesTestUtil.initializePolicyContext(
        getFederationPolicyContext(), getPolicy(), getPolicyInfo(),
        getActiveSubclusters());
  }

  @Test(expected = FederationPolicyInitializationException.class)
  public void testNullWeights() throws Exception {
    getPolicyInfo().setAMRMPolicyWeights(null);
    initializePolicy();
    fail();
  }

  @Test(expected = FederationPolicyInitializationException.class)
  public void testEmptyWeights() throws Exception {
    getPolicyInfo()
        .setAMRMPolicyWeights(new HashMap<SubClusterIdInfo, Float>());
    initializePolicy();
    fail();
  }

  @Test
  public void testSplitBasedOnHeadroom() throws Exception {

    // Tests how the headroom info are used to split based on the capacity
    // each RM claims to give us.
    // Configure policy to be 100% headroom based
    getPolicyInfo().setHeadroomAlpha(1.0f);

    initializePolicy();
    List<ResourceRequest> resourceRequests = createSimpleRequest();

    prepPolicyWithHeadroom();

    Map<SubClusterId, List<ResourceRequest>> response =
        ((FederationAMRMProxyPolicy) getPolicy())
            .splitResourceRequests(resourceRequests);

    // pretty print requests
    LOG.info("Initial headroom");
    prettyPrintRequests(response);

    validateSplit(response, resourceRequests);

    /*
     * based on headroom, we expect 75 containers to got to subcluster0 (60) and
     * subcluster2 (15) according to the advertised headroom (40 and 10), no
     * containers for sublcuster1 as it advertise zero headroom, and 25 to
     * subcluster5 which has unknown headroom, and so it gets 1/4th of the load
     */
    checkExpectedAllocation(response, "subcluster0", 1, 60);
    checkExpectedAllocation(response, "subcluster1", 1, -1);
    checkExpectedAllocation(response, "subcluster2", 1, 15);
    checkExpectedAllocation(response, "subcluster5", 1, 25);
    checkTotalContainerAllocation(response, 100);

    // notify a change in headroom and try again
    AllocateResponse ar = getAllocateResponseWithTargetHeadroom(40);
    ((FederationAMRMProxyPolicy) getPolicy())
        .notifyOfResponse(SubClusterId.newInstance("subcluster2"), ar);
    response = ((FederationAMRMProxyPolicy) getPolicy())
        .splitResourceRequests(resourceRequests);

    LOG.info("After headroom update");
    prettyPrintRequests(response);
    validateSplit(response, resourceRequests);

    /*
     * we simulated a change in headroom for subcluster2, which will now have
     * the same headroom of subcluster0, so each 37.5, note that the odd one
     * will be assigned to either one of the two subclusters
     */
    checkExpectedAllocation(response, "subcluster0", 1, 37);
    checkExpectedAllocation(response, "subcluster1", 1, -1);
    checkExpectedAllocation(response, "subcluster2", 1, 37);
    checkExpectedAllocation(response, "subcluster5", 1, 25);
    checkTotalContainerAllocation(response, 100);
  }

  @Test(timeout = 5000)
  public void testStressPolicy() throws Exception {

    // Tests how the headroom info are used to split based on the capacity
    // each RM claims to give us.
    // Configure policy to be 100% headroom based
    getPolicyInfo().setHeadroomAlpha(1.0f);

    initializePolicy();

    int numRR = 1000;
    List<ResourceRequest> resourceRequests = createLargeRandomList(numRR);

    prepPolicyWithHeadroom();

    int numIterations = 1000;
    long tstart = System.currentTimeMillis();
    for (int i = 0; i < numIterations; i++) {
      Map<SubClusterId, List<ResourceRequest>> response =
          ((FederationAMRMProxyPolicy) getPolicy())
              .splitResourceRequests(resourceRequests);
      validateSplit(response, resourceRequests);
    }
    long tend = System.currentTimeMillis();

    LOG.info("Performed " + numIterations + " policy invocations (and "
        + "validations) in " + (tend - tstart) + "ms");
  }

  @Test
  public void testFWDAllZeroANY() throws Exception {

    // Tests how the headroom info are used to split based on the capacity
    // each RM claims to give us.
    // Configure policy to be 100% headroom based
    getPolicyInfo().setHeadroomAlpha(0.5f);

    initializePolicy();
    List<ResourceRequest> resourceRequests = createZeroSizedANYRequest();

    // this receives responses from sc0,sc1,sc2
    prepPolicyWithHeadroom();

    Map<SubClusterId, List<ResourceRequest>> response =
        ((FederationAMRMProxyPolicy) getPolicy())
            .splitResourceRequests(resourceRequests);

    // we expect all three to appear for a zero-sized ANY

    // pretty print requests
    prettyPrintRequests(response);

    validateSplit(response, resourceRequests);

    // we expect the zero size request to be sent to the first 3 rm (due to
    // the fact that we received responses only from these 3 sublcusters)
    checkExpectedAllocation(response, "subcluster0", 1, 0);
    checkExpectedAllocation(response, "subcluster1", 1, 0);
    checkExpectedAllocation(response, "subcluster2", 1, 0);
    checkExpectedAllocation(response, "subcluster3", -1, -1);
    checkExpectedAllocation(response, "subcluster4", -1, -1);
    checkExpectedAllocation(response, "subcluster5", -1, -1);
    checkTotalContainerAllocation(response, 0);
  }

  @Test
  public void testSplitBasedOnHeadroomAndWeights() throws Exception {

    // Tests how the headroom info are used to split based on the capacity
    // each RM claims to give us.

    // Configure policy to be 50% headroom based and 50% weight based
    getPolicyInfo().setHeadroomAlpha(0.5f);

    initializePolicy();
    List<ResourceRequest> resourceRequests = createSimpleRequest();

    prepPolicyWithHeadroom();

    Map<SubClusterId, List<ResourceRequest>> response =
        ((FederationAMRMProxyPolicy) getPolicy())
            .splitResourceRequests(resourceRequests);

    // pretty print requests
    prettyPrintRequests(response);

    validateSplit(response, resourceRequests);

    // in this case the headroom allocates 50 containers, while weights allocate
    // the rest. due to weights we have 12.5 containers for each
    // sublcuster, the rest is due to headroom.
    checkExpectedAllocation(response, "subcluster0", 1, 42);  // 30 + 12.5
    checkExpectedAllocation(response, "subcluster1", 1, 12);  // 0 + 12.5
    checkExpectedAllocation(response, "subcluster2", 1, 20);  // 7.5 + 12.5
    checkExpectedAllocation(response, "subcluster3", -1, -1);
    checkExpectedAllocation(response, "subcluster4", -1, -1);
    checkExpectedAllocation(response, "subcluster5", 1, 25);  // 12.5 + 12.5
    checkTotalContainerAllocation(response, 100);
  }

  private void prepPolicyWithHeadroom() throws YarnException {
    AllocateResponse ar = getAllocateResponseWithTargetHeadroom(40);
    ((FederationAMRMProxyPolicy) getPolicy())
        .notifyOfResponse(SubClusterId.newInstance("subcluster0"), ar);

    ar = getAllocateResponseWithTargetHeadroom(0);
    ((FederationAMRMProxyPolicy) getPolicy())
        .notifyOfResponse(SubClusterId.newInstance("subcluster1"), ar);

    ar = getAllocateResponseWithTargetHeadroom(10);
    ((FederationAMRMProxyPolicy) getPolicy())
        .notifyOfResponse(SubClusterId.newInstance("subcluster2"), ar);
  }

  private AllocateResponse getAllocateResponseWithTargetHeadroom(
      int numContainers) {
    return AllocateResponse.newInstance(0, null, null,
        Collections.<NodeReport> emptyList(),
        Resource.newInstance(numContainers * 1024, numContainers), null, 10,
        null, Collections.<NMToken> emptyList());
  }

  @Test
  public void testSplitAllocateRequest() throws Exception {

    // Test a complex List<ResourceRequest> is split correctly
    initializePolicy();

    // modify default initialization to include a "homesubcluster"
    // which we will use as the default for when nodes or racks are unknown
    SubClusterInfo sci = mock(SubClusterInfo.class);
    when(sci.getState()).thenReturn(SubClusterState.SC_RUNNING);
    when(sci.getSubClusterId()).thenReturn(getHomeSubCluster());
    getActiveSubclusters().put(getHomeSubCluster(), sci);
    SubClusterIdInfo sc = new SubClusterIdInfo(getHomeSubCluster().getId());

    getPolicyInfo().getRouterPolicyWeights().put(sc, 0.1f);
    getPolicyInfo().getAMRMPolicyWeights().put(sc, 0.1f);

    FederationPoliciesTestUtil.initializePolicyContext(
        getFederationPolicyContext(), getPolicy(), getPolicyInfo(),
        getActiveSubclusters());

    List<ResourceRequest> resourceRequests = createComplexRequest();

    Map<SubClusterId, List<ResourceRequest>> response =
        ((FederationAMRMProxyPolicy) getPolicy())
            .splitResourceRequests(resourceRequests);

    validateSplit(response, resourceRequests);
    prettyPrintRequests(response);

    // we expect 7 entries for home subcluster (2 for request-id 4, 3 for
    // request-id 5, and a part of the broadcast of request-id 2
    checkExpectedAllocation(response, getHomeSubCluster().getId(), 7, 29);

    // for subcluster0 we expect 10 entries, 3 from request-id 0, and 3 from
    // request-id 3, 3 entries from request-id 5, as well as part of the
    // request-id 2 broadast
    checkExpectedAllocation(response, "subcluster0", 10, 32);

    // we expect 5 entries for subcluster1 (4 from request-id 1, and part
    // of the broadcast of request-id 2
    checkExpectedAllocation(response, "subcluster1", 5, 26);

    // sub-cluster 2 should contain 3 entries from request-id 1 and 1 from the
    // broadcast of request-id 2, and no request-id 0
    checkExpectedAllocation(response, "subcluster2", 4, 23);

    // subcluster id 3, 4 should not appear (due to weights or active/inactive)
    checkExpectedAllocation(response, "subcluster3", -1, -1);
    checkExpectedAllocation(response, "subcluster4", -1, -1);

    // subcluster5 should get only part of the request-id 2 broadcast
    checkExpectedAllocation(response, "subcluster5", 1, 20);

    // Check the total number of container asks in all RR
    checkTotalContainerAllocation(response, 130);

    // check that the allocations that show up are what expected
    for (ResourceRequest rr : response.get(getHomeSubCluster())) {
      Assert.assertTrue(
          rr.getAllocationRequestId() == 2L || rr.getAllocationRequestId() == 4L
              || rr.getAllocationRequestId() == 5L);
    }

    List<ResourceRequest> rrs =
        response.get(SubClusterId.newInstance("subcluster0"));
    for (ResourceRequest rr : rrs) {
      Assert.assertTrue(rr.getAllocationRequestId() != 1L);
      Assert.assertTrue(rr.getAllocationRequestId() != 4L);
    }

    for (ResourceRequest rr : response
        .get(SubClusterId.newInstance("subcluster1"))) {
      Assert.assertTrue(rr.getAllocationRequestId() == 1L
          || rr.getAllocationRequestId() == 2L);
    }

    for (ResourceRequest rr : response
        .get(SubClusterId.newInstance("subcluster2"))) {
      Assert.assertTrue(rr.getAllocationRequestId() == 1L
          || rr.getAllocationRequestId() == 2L);
    }

    for (ResourceRequest rr : response
        .get(SubClusterId.newInstance("subcluster5"))) {
      Assert.assertTrue(rr.getAllocationRequestId() == 2);
      Assert.assertTrue(rr.getRelaxLocality());
    }
  }

  // check that the number of containers in the first ResourceRequest in
  // response for this sub-cluster matches expectations. -1 indicate the
  // response should be null
  private void checkExpectedAllocation(
      Map<SubClusterId, List<ResourceRequest>> response, String subCluster,
      long totResourceRequests, long minimumTotalContainers) {
    if (minimumTotalContainers == -1) {
      Assert.assertNull(response.get(SubClusterId.newInstance(subCluster)));
    } else {
      SubClusterId sc = SubClusterId.newInstance(subCluster);
      Assert.assertEquals(totResourceRequests, response.get(sc).size());

      long actualContCount = 0;
      for (ResourceRequest rr : response.get(sc)) {
        actualContCount += rr.getNumContainers();
      }
      Assert.assertTrue(
          "Actual count " + actualContCount + " should be at least "
              + minimumTotalContainers,
          minimumTotalContainers <= actualContCount);
    }
  }

  private void checkTotalContainerAllocation(
      Map<SubClusterId, List<ResourceRequest>> response, long totalContainers) {
    long actualContCount = 0;
    for (Map.Entry<SubClusterId, List<ResourceRequest>> entry : response
        .entrySet()) {
      for (ResourceRequest rr : entry.getValue()) {
        actualContCount += rr.getNumContainers();
      }
    }
    Assert.assertEquals(totalContainers, actualContCount);
  }

  private void validateSplit(Map<SubClusterId, List<ResourceRequest>> split,
      List<ResourceRequest> original) throws YarnException {

    SubClusterResolver resolver =
        getFederationPolicyContext().getFederationSubclusterResolver();

    // Apply general validation rules
    int numUsedSubclusters = split.size();

    Set<Long> originalIds = new HashSet<>();
    Set<Long> splitIds = new HashSet<>();

    int originalContainers = 0;
    for (ResourceRequest rr : original) {
      originalContainers += rr.getNumContainers();
      originalIds.add(rr.getAllocationRequestId());
    }

    int splitContainers = 0;
    for (Map.Entry<SubClusterId, List<ResourceRequest>> rrs : split
        .entrySet()) {
      for (ResourceRequest rr : rrs.getValue()) {
        splitContainers += rr.getNumContainers();
        splitIds.add(rr.getAllocationRequestId());
        // check node-local asks are sent to right RM (only)
        SubClusterId fid = null;
        try {
          fid = resolver.getSubClusterForNode(rr.getResourceName());
        } catch (YarnException e) {
          // ignore code will handle
        }
        if (!rrs.getKey().equals(getHomeSubCluster()) && fid != null
            && !fid.equals(rrs.getKey())) {
          Assert.fail("A node-local (or resolvable rack-local) RR should not "
              + "be send to an RM other than what it resolves to.");
        }
      }
    }

    // check we are not inventing Allocation Ids
    Assert.assertEquals(originalIds, splitIds);

    // check we are not exceedingly replicating the container asks among
    // RMs (a little is allowed due to rounding of fractional splits)
    Assert.assertTrue(
        " Containers requested (" + splitContainers + ") should "
            + "not exceed the original count of containers ("
            + originalContainers + ") by more than the number of subclusters ("
            + numUsedSubclusters + ")",
        originalContainers + numUsedSubclusters >= splitContainers);

    // Test target Ids
    for (SubClusterId targetId : split.keySet()) {
      Assert.assertTrue("Target subclusters should be in the active set",
          getActiveSubclusters().containsKey(targetId));
      Assert.assertTrue(
          "Target subclusters (" + targetId + ") should have weight >0 in "
              + "the policy ",
          getPolicyInfo().getRouterPolicyWeights()
              .get(new SubClusterIdInfo(targetId)) > 0);
    }
  }

  private void prettyPrintRequests(
      Map<SubClusterId, List<ResourceRequest>> response) {
    for (Map.Entry<SubClusterId, List<ResourceRequest>> entry : response
        .entrySet()) {
      String str = "";
      for (ResourceRequest rr : entry.getValue()) {
        str += " [id:" + rr.getAllocationRequestId() + " loc:"
            + rr.getResourceName() + " numCont:" + rr.getNumContainers()
            + "], ";
      }
      LOG.info(entry.getKey() + " --> " + str);
    }
  }

  private List<ResourceRequest> createLargeRandomList(int numRR)
      throws Exception {

    List<ResourceRequest> out = new ArrayList<>();
    Random rand = new Random(1);
    DefaultSubClusterResolverImpl resolver =
        (DefaultSubClusterResolverImpl) getFederationPolicyContext()
            .getFederationSubclusterResolver();

    List<String> nodes =
        new ArrayList<>(resolver.getNodeToSubCluster().keySet());

    for (int i = 0; i < numRR; i++) {
      String nodeName = nodes.get(rand.nextInt(nodes.size()));
      long allocationId = (long) rand.nextInt(20);

      // create a single container request in sc0
      out.add(FederationPoliciesTestUtil.createResourceRequest(allocationId,
          nodeName, 1024, 1, 1, rand.nextInt(100), null, rand.nextBoolean()));
    }
    return out;
  }

  private List<ResourceRequest> createSimpleRequest() throws Exception {

    List<ResourceRequest> out = new ArrayList<>();

    // create a single container request in sc0
    out.add(FederationPoliciesTestUtil.createResourceRequest(0L,
        ResourceRequest.ANY, 1024, 1, 1, 100, null, true));
    return out;
  }

  private List<ResourceRequest> createZeroSizedANYRequest() throws Exception {

    List<ResourceRequest> out = new ArrayList<>();

    // create a single container request in sc0
    out.add(FederationPoliciesTestUtil.createResourceRequest(0L,
        ResourceRequest.ANY, 1024, 1, 1, 0, null, true));
    return out;
  }

  private List<ResourceRequest> createComplexRequest() throws Exception {

    List<ResourceRequest> out = new ArrayList<>();

    // create a single container request in sc0
    out.add(FederationPoliciesTestUtil.createResourceRequest(0L,
        "subcluster0-rack0-host0", 1024, 1, 1, 1, null, false));
    out.add(FederationPoliciesTestUtil.createResourceRequest(0L,
        "subcluster0-rack0", 1024, 1, 1, 1, null, false));
    out.add(FederationPoliciesTestUtil.createResourceRequest(0L,
        ResourceRequest.ANY, 1024, 1, 1, 1, null, false));

    // create a single container request with 3 alternative hosts across sc1,sc2
    // where we want 2 containers in sc1 and 1 in sc2
    out.add(FederationPoliciesTestUtil.createResourceRequest(1L,
        "subcluster1-rack1-host1", 1024, 1, 1, 1, null, false));
    out.add(FederationPoliciesTestUtil.createResourceRequest(1L,
        "subcluster1-rack1-host2", 1024, 1, 1, 1, null, false));
    out.add(FederationPoliciesTestUtil.createResourceRequest(1L,
        "subcluster2-rack3-host3", 1024, 1, 1, 1, null, false));
    out.add(FederationPoliciesTestUtil.createResourceRequest(1L,
        "subcluster1-rack1", 1024, 1, 1, 2, null, false));
    out.add(FederationPoliciesTestUtil.createResourceRequest(1L,
        "subcluster2-rack3", 1024, 1, 1, 1, null, false));
    out.add(FederationPoliciesTestUtil.createResourceRequest(1L,
        ResourceRequest.ANY, 1024, 1, 1, 3, null, false));

    // create a non-local ANY request that can span anything
    out.add(FederationPoliciesTestUtil.createResourceRequest(2L,
        ResourceRequest.ANY, 1024, 1, 1, 100, null, true));

    // create a single container request in sc0 with relaxed locality
    out.add(FederationPoliciesTestUtil.createResourceRequest(3L,
        "subcluster0-rack0-host0", 1024, 1, 1, 1, null, true));
    out.add(FederationPoliciesTestUtil.createResourceRequest(3L,
        "subcluster0-rack0", 1024, 1, 1, 1, null, true));
    out.add(FederationPoliciesTestUtil.createResourceRequest(3L,
        ResourceRequest.ANY, 1024, 1, 1, 1, null, true));

    // create a request of an unknown node/rack and expect this to show up
    // in homesubcluster
    out.add(FederationPoliciesTestUtil.createResourceRequest(4L, "unknownNode",
        1024, 1, 1, 1, null, false));
    out.add(FederationPoliciesTestUtil.createResourceRequest(4L, "unknownRack",
        1024, 1, 1, 1, null, false));
    out.add(FederationPoliciesTestUtil.createResourceRequest(4L,
        ResourceRequest.ANY, 1024, 1, 1, 1, null, false));

    // create a request of two hosts, an unknown node and a known node, both in
    // a known rack, and expect the unknown node to show up in homesubcluster
    out.add(FederationPoliciesTestUtil.createResourceRequest(5L,
        "subcluster0-rack0-host0", 1024, 1, 1, 2, null, false));
    out.add(FederationPoliciesTestUtil.createResourceRequest(5L,
        "subcluster0-rack0", 1024, 1, 1, 2, null, false));
    out.add(FederationPoliciesTestUtil.createResourceRequest(5L, "node4", 1024,
        1, 1, 2, null, false));
    out.add(FederationPoliciesTestUtil.createResourceRequest(5L, "rack2", 1024,
        1, 1, 2, null, false));
    out.add(FederationPoliciesTestUtil.createResourceRequest(5L,
        ResourceRequest.ANY, 1024, 1, 1, 4, null, false));

    return out;
  }

  public String printList(ArrayList<Integer> list) {
    StringBuilder sb = new StringBuilder();
    for (Integer entry : list) {
      sb.append(entry + ", ");
    }
    return sb.toString();
  }

  @Test
  public void testIntegerAssignment() throws YarnException {
    float[] weights =
        new float[] {0, 0.1f, 0.2f, 0.2f, -0.1f, 0.1f, 0.2f, 0.1f, 0.1f};
    int[] expectedMin = new int[] {0, 1, 3, 3, 0, 1, 3, 1, 1};
    ArrayList<Float> weightsList = new ArrayList<>();
    for (float weight : weights) {
      weightsList.add(weight);
    }

    LocalityMulticastAMRMProxyPolicy policy =
        (LocalityMulticastAMRMProxyPolicy) getPolicy();
    for (int i = 0; i < 500000; i++) {
      ArrayList<Integer> allocations =
          policy.computeIntegerAssignment(19, weightsList);
      int sum = 0;
      for (int j = 0; j < weights.length; j++) {
        sum += allocations.get(j);
        if (allocations.get(j) < expectedMin[j]) {
          Assert.fail(allocations.get(j) + " at index " + j
              + " should be at least " + expectedMin[j] + ". Allocation array: "
              + printList(allocations));
        }
      }
      Assert.assertEquals(
          "Expect sum to be 19 in array: " + printList(allocations), 19, sum);
    }
  }

  @Test
  public void testCancelWithLocalizedResource() throws YarnException {
    // Configure policy to be 100% headroom based
    getPolicyInfo().setHeadroomAlpha(1.0f);

    initializePolicy();
    List<ResourceRequest> resourceRequests = new ArrayList<>();

    // Initialize the headroom map
    prepPolicyWithHeadroom();

    // Cancel at ANY level only
    resourceRequests.add(FederationPoliciesTestUtil.createResourceRequest(0L,
        "subcluster0-rack0-host0", 1024, 1, 1, 1, null, false));
    resourceRequests.add(FederationPoliciesTestUtil.createResourceRequest(0L,
        "subcluster0-rack0", 1024, 1, 1, 1, null, false));
    resourceRequests.add(FederationPoliciesTestUtil.createResourceRequest(0L,
        ResourceRequest.ANY, 1024, 1, 1, 0, null, false));

    Map<SubClusterId, List<ResourceRequest>> response =
        ((FederationAMRMProxyPolicy) getPolicy())
            .splitResourceRequests(resourceRequests);

    checkExpectedAllocation(response, "subcluster0", 3, 1);
    checkExpectedAllocation(response, "subcluster1", 1, 0);
    checkExpectedAllocation(response, "subcluster2", 1, 0);
    checkExpectedAllocation(response, "subcluster3", -1, -1);
    checkExpectedAllocation(response, "subcluster4", -1, -1);
    checkExpectedAllocation(response, "subcluster5", -1, -1);

    resourceRequests.clear();
    // Cancel at node level only
    resourceRequests.add(FederationPoliciesTestUtil.createResourceRequest(0L,
        "subcluster0-rack0-host0", 1024, 1, 1, 0, null, false));
    resourceRequests.add(FederationPoliciesTestUtil.createResourceRequest(0L,
        "subcluster0-rack0", 1024, 1, 1, 0, null, false));
    resourceRequests.add(FederationPoliciesTestUtil.createResourceRequest(0L,
        ResourceRequest.ANY, 1024, 1, 1, 100, null, false));

    response = ((FederationAMRMProxyPolicy) getPolicy())
        .splitResourceRequests(resourceRequests);

    /*
     * Since node request is a cancel, it should not be considered associated
     * with localized requests. Based on headroom, we expect 75 containers to
     * got to subcluster0 (60) and subcluster2 (15) according to the advertised
     * headroom (40 and 10), no containers for sublcuster1 as it advertise zero
     * headroom, and 25 to subcluster5 which has unknown headroom, and so it
     * gets 1/4th of the load
     */
    checkExpectedAllocation(response, "subcluster0", 3, 60);
    checkExpectedAllocation(response, "subcluster1", 1, -1);
    checkExpectedAllocation(response, "subcluster2", 1, 15);
    checkExpectedAllocation(response, "subcluster5", 1, 25);
    checkTotalContainerAllocation(response, 100);
  }
}