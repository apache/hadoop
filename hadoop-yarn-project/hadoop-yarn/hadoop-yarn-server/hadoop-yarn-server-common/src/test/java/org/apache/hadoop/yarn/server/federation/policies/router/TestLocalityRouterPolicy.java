/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.policies.router;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.utils.FederationPoliciesTestUtil;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class to validate the correctness of LocalityRouterPolicy.
 */
public class TestLocalityRouterPolicy extends TestWeightedRandomRouterPolicy {

  /*
   * The MachineList for the default Resolver has the following nodes:
   *
   * node1<=>subcluster1
   *
   * node2<=>subcluster2
   *
   * noDE3<=>subcluster3
   *
   * node4<=>subcluster3
   *
   * subcluster0-rack0-host0<=>subcluster0
   *
   * Subcluster1-RACK1-HOST1<=>subcluster1
   *
   * SUBCLUSTER1-RACK1-HOST2<=>subcluster1
   *
   * SubCluster2-RACK3-HOST3<=>subcluster2
   */

  @Before
  public void setUp() throws Exception {
    setPolicy(new LocalityRouterPolicy());
    setPolicyInfo(new WeightedPolicyInfo());

    configureWeights(4);

    initializePolicy(new YarnConfiguration());
  }

  private void initializePolicy(Configuration conf) throws YarnException {
    setFederationPolicyContext(new FederationPolicyInitializationContext());
    SubClusterResolver resolver = FederationPoliciesTestUtil.initResolver();
    getFederationPolicyContext().setFederationSubclusterResolver(resolver);
    ByteBuffer buf = getPolicyInfo().toByteBuffer();
    getFederationPolicyContext().setSubClusterPolicyConfiguration(
        SubClusterPolicyConfiguration
            .newInstance("queue1", getPolicy().getClass().getCanonicalName(),
                buf));
    getFederationPolicyContext().setHomeSubcluster(getHomeSubCluster());
    FederationPoliciesTestUtil
        .initializePolicyContext(getFederationPolicyContext(), getPolicy(),
            getPolicyInfo(), getActiveSubclusters(), conf);
  }

  /**
   * This test validates the correctness in case of the request has 1 node and
   * the node belongs to an active subcluster.
   */
  @Test
  public void testNodeInActiveSubCluster() throws YarnException {
    List<ResourceRequest> requests = new ArrayList<ResourceRequest>();
    requests.add(ResourceRequest
        .newInstance(Priority.UNDEFINED, "node1", Resource.newInstance(10, 1),
            1));
    requests.add(ResourceRequest
        .newInstance(Priority.UNDEFINED, "rack1", Resource.newInstance(10, 1),
            1));
    requests.add(ResourceRequest
        .newInstance(Priority.UNDEFINED, ResourceRequest.ANY,
            Resource.newInstance(10, 1), 1));
    ApplicationSubmissionContext asc = ApplicationSubmissionContext
        .newInstance(null, null, null, null, null, false, false, 0,
            Resources.none(), null, false, null, null);
    asc.setAMContainerResourceRequests(requests);

    SubClusterId chosen =
        ((FederationRouterPolicy) getPolicy()).getHomeSubcluster(asc, null);
    // If node1 is active, we should choose the sub cluster with node1
    if (getActiveSubclusters().containsKey(
        getFederationPolicyContext().getFederationSubclusterResolver()
            .getSubClusterForNode("node1").getId())) {
      Assert.assertEquals(
          getFederationPolicyContext().getFederationSubclusterResolver()
              .getSubClusterForNode("node1"), chosen);
    }
    // Regardless, we should choose an active SubCluster
    Assert.assertTrue(getActiveSubclusters().containsKey(chosen));
  }

  /**
   * This test validates the correctness in case of the request has multiple
   * ResourceRequests. The tests without ResourceRequests are done in
   * TestWeightedRandomRouterPolicy.
   */
  @Test
  public void testMultipleResourceRequests() throws YarnException {
    List<ResourceRequest> requests = new ArrayList<ResourceRequest>();
    requests.add(ResourceRequest
        .newInstance(Priority.UNDEFINED, "node1", Resource.newInstance(10, 1),
            1));
    requests.add(ResourceRequest
        .newInstance(Priority.UNDEFINED, "node2", Resource.newInstance(10, 1),
            1));
    ApplicationSubmissionContext asc = ApplicationSubmissionContext
        .newInstance(null, null, null, null, null, false, false, 0,
            Resources.none(), null, false, null, null);
    asc.setAMContainerResourceRequests(requests);
    try {
      ((FederationRouterPolicy) getPolicy()).getHomeSubcluster(asc, null);
      Assert.fail();
    } catch (FederationPolicyException e) {
      Assert.assertTrue(
          e.getMessage().startsWith("Invalid number of resource requests: "));
    }
  }

  /**
   * This test validates the correctness in case of the request has 1 node and
   * the node does not exist in the Resolver MachineList file.
   */
  @Test
  public void testNodeNotExists() throws YarnException {
    List<ResourceRequest> requests = new ArrayList<ResourceRequest>();
    boolean relaxLocality = true;
    requests.add(ResourceRequest
        .newInstance(Priority.UNDEFINED, "node5", Resource.newInstance(10, 1),
            1, relaxLocality));
    requests.add(ResourceRequest
        .newInstance(Priority.UNDEFINED, "rack1", Resource.newInstance(10, 1),
            1));
    requests.add(ResourceRequest
        .newInstance(Priority.UNDEFINED, ResourceRequest.ANY,
            Resource.newInstance(10, 1), 1));
    ApplicationSubmissionContext asc = ApplicationSubmissionContext
        .newInstance(null, null, null, null, null, false, false, 0,
            Resources.none(), null, false, null, null);
    asc.setAMContainerResourceRequests(requests);

    try {
      ((FederationRouterPolicy) getPolicy()).getHomeSubcluster(asc, null);
    } catch (FederationPolicyException e) {
      Assert.fail();
    }
  }

  /**
   * This test validates the correctness in case of the request has 1 node and
   * the node is in a blacklist subclusters.
   */
  @Test
  public void testNodeInABlacklistSubCluster() throws YarnException {
    // Blacklist SubCluster3
    String subClusterToBlacklist = "subcluster3";
    // Remember the current value of subcluster3
    Float value =
        getPolicyInfo().getRouterPolicyWeights().get(subClusterToBlacklist);
    getPolicyInfo().getRouterPolicyWeights()
        .put(new SubClusterIdInfo(subClusterToBlacklist), 0.0f);
    initializePolicy(new YarnConfiguration());

    FederationPoliciesTestUtil
        .initializePolicyContext(getFederationPolicyContext(), getPolicy(),
            getPolicyInfo(), getActiveSubclusters(), new Configuration());

    List<ResourceRequest> requests = new ArrayList<ResourceRequest>();
    boolean relaxLocality = true;
    requests.add(ResourceRequest
        .newInstance(Priority.UNDEFINED, "node4", Resource.newInstance(10, 1),
            1, relaxLocality));
    requests.add(ResourceRequest
        .newInstance(Priority.UNDEFINED, "rack1", Resource.newInstance(10, 1),
            1));
    requests.add(ResourceRequest
        .newInstance(Priority.UNDEFINED, ResourceRequest.ANY,
            Resource.newInstance(10, 1), 1));
    ApplicationSubmissionContext asc = ApplicationSubmissionContext
        .newInstance(null, null, null, null, null, false, false, 0,
            Resources.none(), null, false, null, null);
    asc.setAMContainerResourceRequests(requests);

    try {
      SubClusterId targetId =
          ((FederationRouterPolicy) getPolicy()).getHomeSubcluster(asc, null);
      // The selected subcluster HAS no to be the same as the one blacklisted.
      Assert.assertNotEquals(targetId.getId(), subClusterToBlacklist);
    } catch (FederationPolicyException e) {
      Assert.fail();
    }

    // Set again the previous value for the other tests
    getPolicyInfo().getRouterPolicyWeights()
        .put(new SubClusterIdInfo(subClusterToBlacklist), value);
  }

  /**
   * This test validates the correctness in case of the request has 1 node and
   * the node is not in the policy weights.
   */
  @Test
  public void testNodeNotInPolicy() throws YarnException {
    // Blacklist SubCluster3
    String subClusterToBlacklist = "subcluster3";
    // Remember the current value of subcluster3
    Float value =
        getPolicyInfo().getRouterPolicyWeights().get(subClusterToBlacklist);
    getPolicyInfo().getRouterPolicyWeights().remove(subClusterToBlacklist);
    initializePolicy(new YarnConfiguration());

    FederationPoliciesTestUtil
        .initializePolicyContext(getFederationPolicyContext(), getPolicy(),
            getPolicyInfo(), getActiveSubclusters(), new Configuration());

    List<ResourceRequest> requests = new ArrayList<ResourceRequest>();
    boolean relaxLocality = true;
    requests.add(ResourceRequest
        .newInstance(Priority.UNDEFINED, "node4", Resource.newInstance(10, 1),
            1, relaxLocality));
    requests.add(ResourceRequest
        .newInstance(Priority.UNDEFINED, "rack1", Resource.newInstance(10, 1),
            1));
    requests.add(ResourceRequest
        .newInstance(Priority.UNDEFINED, ResourceRequest.ANY,
            Resource.newInstance(10, 1), 1));
    ApplicationSubmissionContext asc = ApplicationSubmissionContext
        .newInstance(null, null, null, null, null, false, false, 0,
            Resources.none(), null, false, null, null);
    asc.setAMContainerResourceRequests(requests);

    try {
      SubClusterId targetId =
          ((FederationRouterPolicy) getPolicy()).getHomeSubcluster(asc, null);
      // The selected subcluster HAS no to be the same as the one blacklisted.
      Assert.assertNotEquals(targetId.getId(), subClusterToBlacklist);
    } catch (FederationPolicyException e) {
      Assert.fail();
    }

    // Set again the previous value for the other tests
    getPolicyInfo().getRouterPolicyWeights()
        .put(new SubClusterIdInfo(subClusterToBlacklist), value);
  }
}

